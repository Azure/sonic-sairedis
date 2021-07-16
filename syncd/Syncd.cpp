#include "Syncd.h"
#include "VidManager.h"
#include "NotificationHandler.h"
#include "Workaround.h"
#include "ComparisonLogic.h"
#include "HardReiniter.h"
#include "RedisClient.h"
#include "RequestShutdown.h"
#include "WarmRestartTable.h"
#include "ContextConfigContainer.h"
#include "BreakConfigParser.h"
#include "RedisNotificationProducer.h"
#include "ZeroMQNotificationProducer.h"
#include "RedisSelectableChannel.h"
#include "ZeroMQSelectableChannel.h"
#include "PerformanceIntervalTimer.h"
#include "TimerWatchdog.h"

#include "sairediscommon.h"

#include "swss/logger.h"
#include "swss/select.h"
#include "swss/tokenize.h"
#include "swss/notificationproducer.h"

#include "meta/sai_serialize.h"

#include "vslib/inc/saivs.h"

#include <unistd.h>
#include <inttypes.h>

#include <iterator>
#include <algorithm>

#define DEF_SAI_WARM_BOOT_DATA_FILE "/var/warmboot/sai-warmboot.bin"

using namespace syncd;
using namespace saimeta;
using namespace sairediscommon;
using namespace std::placeholders;

Syncd::Syncd(
        _In_ std::shared_ptr<sairedis::SaiInterface> vendorSai,
        _In_ std::shared_ptr<CommandLineOptions> cmd,
        _In_ bool isWarmStart):
    m_commandLineOptions(cmd),
    m_isWarmStart(isWarmStart),
    m_firstInitWasPerformed(false),
    m_asicInitViewMode(false), // by default we are in APPLY view mode
    m_vendorSai(vendorSai),
    m_veryFirstRun(false),
    m_enableSyncMode(false)
{
    SWSS_LOG_ENTER();

    setSaiApiLogLevel();

    SWSS_LOG_NOTICE("command line: %s", m_commandLineOptions->getCommandLineString().c_str());

    auto ccc = sairedis::ContextConfigContainer::loadFromFile(m_commandLineOptions->m_contextConfig.c_str());

    m_contextConfig = ccc->get(m_commandLineOptions->m_globalContext);

    if (m_contextConfig == nullptr)
    {
        SWSS_LOG_THROW("no context config defined at global context %u", m_commandLineOptions->m_globalContext);
    }

    if (m_contextConfig->m_zmqEnable && m_commandLineOptions->m_enableSyncMode)
    {
        SWSS_LOG_NOTICE("disabling command line sync mode, since context zmq enabled");

        m_commandLineOptions->m_enableSyncMode = false;

        m_commandLineOptions->m_redisCommunicationMode = SAI_REDIS_COMMUNICATION_MODE_ZMQ_SYNC;
    }

    if (m_commandLineOptions->m_enableSyncMode)
    {
        SWSS_LOG_WARN("enable sync mode is deprecated, please use communication mode, FORCING redis sync mode");

        m_enableSyncMode = true;

        m_contextConfig->m_zmqEnable = false;

        m_commandLineOptions->m_redisCommunicationMode = SAI_REDIS_COMMUNICATION_MODE_REDIS_SYNC;
    }

    if (m_commandLineOptions->m_redisCommunicationMode == SAI_REDIS_COMMUNICATION_MODE_ZMQ_SYNC)
    {
        SWSS_LOG_NOTICE("zmq sync mode enabled via cmd line");

        m_contextConfig->m_zmqEnable = true;

        m_enableSyncMode = true;
    }

    m_manager = std::make_shared<FlexCounterManager>(m_vendorSai, m_contextConfig->m_dbCounters);

    loadProfileMap();

    m_profileIter = m_profileMap.begin();

    // we need STATE_DB ASIC_DB and COUNTERS_DB

    m_dbAsic = std::make_shared<swss::DBConnector>(m_contextConfig->m_dbAsic, 0);

    if (m_contextConfig->m_zmqEnable)
    {
        m_notifications = std::make_shared<ZeroMQNotificationProducer>(m_contextConfig->m_zmqNtfEndpoint);

        SWSS_LOG_NOTICE("zmq enabled, forcing sync mode");

        m_enableSyncMode = true;

        m_selectableChannel = std::make_shared<ZeroMQSelectableChannel>(m_contextConfig->m_zmqEndpoint);
    }
    else
    {
        m_notifications = std::make_shared<RedisNotificationProducer>(m_contextConfig->m_dbAsic);

        m_enableSyncMode = m_commandLineOptions->m_redisCommunicationMode == SAI_REDIS_COMMUNICATION_MODE_REDIS_SYNC;

        bool modifyRedis = m_enableSyncMode ? false : true;

        m_selectableChannel = std::make_shared<RedisSelectableChannel>(
                m_dbAsic,
                ASIC_STATE_TABLE,
                REDIS_TABLE_GETRESPONSE,
                TEMP_PREFIX,
                modifyRedis);
    }

    m_client = std::make_shared<RedisClient>(m_dbAsic);

    m_processor = std::make_shared<NotificationProcessor>(m_notifications, m_client, std::bind(&Syncd::syncProcessNotification, this, _1));
    m_handler = std::make_shared<NotificationHandler>(m_processor);

    m_sn.onFdbEvent = std::bind(&NotificationHandler::onFdbEvent, m_handler.get(), _1, _2);
    m_sn.onPortStateChange = std::bind(&NotificationHandler::onPortStateChange, m_handler.get(), _1, _2);
    m_sn.onQueuePfcDeadlock = std::bind(&NotificationHandler::onQueuePfcDeadlock, m_handler.get(), _1, _2);
    m_sn.onSwitchShutdownRequest = std::bind(&NotificationHandler::onSwitchShutdownRequest, m_handler.get(), _1);
    m_sn.onSwitchStateChange = std::bind(&NotificationHandler::onSwitchStateChange, m_handler.get(), _1, _2);

    m_handler->setSwitchNotifications(m_sn.getSwitchNotifications());

    m_restartQuery = std::make_shared<swss::NotificationConsumer>(m_dbAsic.get(), SYNCD_NOTIFICATION_CHANNEL_RESTARTQUERY);

    // TODO to be moved to ASIC_DB
    m_dbFlexCounter = std::make_shared<swss::DBConnector>(m_contextConfig->m_dbFlex, 0);
    m_flexCounter = std::make_shared<swss::ConsumerTable>(m_dbFlexCounter.get(), FLEX_COUNTER_TABLE);
    m_flexCounterGroup = std::make_shared<swss::ConsumerTable>(m_dbFlexCounter.get(), FLEX_COUNTER_GROUP_TABLE);

    m_switchConfigContainer = std::make_shared<sairedis::SwitchConfigContainer>();
    m_redisVidIndexGenerator = std::make_shared<sairedis::RedisVidIndexGenerator>(m_dbAsic, REDIS_KEY_VIDCOUNTER);

    m_virtualObjectIdManager =
        std::make_shared<sairedis::VirtualObjectIdManager>(
                m_commandLineOptions->m_globalContext,
                m_switchConfigContainer,
                m_redisVidIndexGenerator);

    // TODO move to syncd object
    m_translator = std::make_shared<VirtualOidTranslator>(m_client, m_virtualObjectIdManager,  vendorSai);

    m_processor->m_translator = m_translator; // TODO as param

    m_veryFirstRun = isVeryFirstRun();

    performStartupLogic();

    m_smt.profileGetValue = std::bind(&Syncd::profileGetValue, this, _1, _2);
    m_smt.profileGetNextValue = std::bind(&Syncd::profileGetNextValue, this, _1, _2, _3);

    m_test_services = m_smt.getServiceMethodTable();

    sai_status_t status = vendorSai->initialize(0, &m_test_services);

    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_ERROR("FATAL: failed to sai_api_initialize: %s",
                sai_serialize_status(status).c_str());

        abort();
    }

    m_breakConfig = BreakConfigParser::parseBreakConfig(m_commandLineOptions->m_breakConfig);

    SWSS_LOG_NOTICE("syncd started");
}

Syncd::~Syncd()
{
    SWSS_LOG_ENTER();

    // empty
}

void Syncd::performStartupLogic()
{
    SWSS_LOG_ENTER();

    // ignore warm logic here if syncd starts in Mellanox fastfast boot mode

    if (m_isWarmStart && (m_commandLineOptions->m_startType != SAI_START_TYPE_FASTFAST_BOOT))
    {
        SWSS_LOG_WARN("override command line startType=%s via SAI_START_TYPE_WARM_BOOT",
                CommandLineOptions::startTypeToString(m_commandLineOptions->m_startType).c_str());

        m_commandLineOptions->m_startType = SAI_START_TYPE_WARM_BOOT;
    }

    if (m_commandLineOptions->m_startType == SAI_START_TYPE_WARM_BOOT)
    {
        const char *warmBootReadFile = profileGetValue(0, SAI_KEY_WARM_BOOT_READ_FILE);

        SWSS_LOG_NOTICE("using warmBootReadFile: '%s'", warmBootReadFile);

        if (warmBootReadFile == NULL || access(warmBootReadFile, F_OK) == -1)
        {
            SWSS_LOG_WARN("user requested warmStart but warmBootReadFile is not specified or not accessible, forcing cold start");

            m_commandLineOptions->m_startType = SAI_START_TYPE_COLD_BOOT;
        }
    }

    if (m_commandLineOptions->m_startType == SAI_START_TYPE_WARM_BOOT && m_veryFirstRun)
    {
        SWSS_LOG_WARN("warm start requested, but this is very first syncd start, forcing cold start");

        /*
         * We force cold start since if it's first run then redis db is not
         * complete so redis asic view will not reflect warm boot asic state,
         * if this happen then orch agent needs to be restarted as well to
         * repopulate asic view.
         */

        m_commandLineOptions->m_startType = SAI_START_TYPE_COLD_BOOT;
    }

    if (m_commandLineOptions->m_startType == SAI_START_TYPE_FASTFAST_BOOT)
    {
        /*
         * Mellanox SAI requires to pass SAI_WARM_BOOT as SAI_BOOT_KEY
         * to start 'fastfast'
         */

        m_profileMap[SAI_KEY_BOOT_TYPE] = std::to_string(SAI_START_TYPE_WARM_BOOT);
    }
    else
    {
        m_profileMap[SAI_KEY_BOOT_TYPE] = std::to_string(m_commandLineOptions->m_startType); // number value is needed
    }
}

bool Syncd::getAsicInitViewMode() const
{
    SWSS_LOG_ENTER();

    return m_asicInitViewMode;
}

void Syncd::setAsicInitViewMode(
        _In_ bool enable)
{
    SWSS_LOG_ENTER();

    m_asicInitViewMode = enable;
}

bool Syncd::isInitViewMode() const
{
    SWSS_LOG_ENTER();

    return m_asicInitViewMode && m_commandLineOptions->m_enableTempView;
}

void Syncd::processEvent(
        _In_ SelectableChannel& consumer)
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    do
    {
        swss::KeyOpFieldsValuesTuple kco;

        /*
         * In init mode we put all data to TEMP view and we snoop.  We need
         * to specify temporary view prefix in consumer since consumer puts
         * data to redis db.
         */

        consumer.pop(kco, isInitViewMode());

        processSingleEvent(kco);
    }
    while (!consumer.empty());
}

sai_status_t Syncd::processSingleEvent(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    auto& key = kfvKey(kco);
    auto& op = kfvOp(kco);

    SWSS_LOG_INFO("key: %s op: %s", key.c_str(), op.c_str());

    if (key.length() == 0)
    {
        SWSS_LOG_DEBUG("no elements in m_buffer");

        return SAI_STATUS_SUCCESS;
    }

    if (op == REDIS_ASIC_STATE_COMMAND_CREATE)
        return processQuadEvent(SAI_COMMON_API_CREATE, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_REMOVE)
        return processQuadEvent(SAI_COMMON_API_REMOVE, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_SET)
        return processQuadEvent(SAI_COMMON_API_SET, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_GET)
        return processQuadEvent(SAI_COMMON_API_GET, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_BULK_CREATE)
        return processBulkQuadEvent(SAI_COMMON_API_BULK_CREATE, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_BULK_REMOVE)
        return processBulkQuadEvent(SAI_COMMON_API_BULK_REMOVE, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_BULK_SET)
        return processBulkQuadEvent(SAI_COMMON_API_BULK_SET, kco);

    if (op == REDIS_ASIC_STATE_COMMAND_NOTIFY)
        return processNotifySyncd(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_GET_STATS)
        return processGetStatsEvent(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_CLEAR_STATS)
        return processClearStatsEvent(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_FLUSH)
        return processFdbFlush(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_ATTR_CAPABILITY_QUERY)
        return processAttrCapabilityQuery(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_ATTR_ENUM_VALUES_CAPABILITY_QUERY)
        return processAttrEnumValuesCapabilityQuery(kco);

    if (op == REDIS_ASIC_STATE_COMMAND_OBJECT_TYPE_GET_AVAILABILITY_QUERY)
        return processObjectTypeGetAvailabilityQuery(kco);

    SWSS_LOG_THROW("event op '%s' is not implemented, FIXME", op.c_str());
}

sai_status_t Syncd::processAttrCapabilityQuery(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    auto& strSwitchVid = kfvKey(kco);

    sai_object_id_t switchVid;
    sai_deserialize_object_id(strSwitchVid, switchVid);

    sai_object_id_t switchRid = m_translator->translateVidToRid(switchVid);

    auto& values = kfvFieldsValues(kco);

    if (values.size() != 2)
    {
        SWSS_LOG_ERROR("Invalid input: expected 2 arguments, received %zu", values.size());

        m_selectableChannel->set(sai_serialize_status(SAI_STATUS_INVALID_PARAMETER), {}, REDIS_ASIC_STATE_COMMAND_ATTR_CAPABILITY_RESPONSE);

        return SAI_STATUS_INVALID_PARAMETER;
    }

    sai_object_type_t objectType;
    sai_deserialize_object_type(fvValue(values[0]), objectType);

    sai_attr_id_t attrId;
    sai_deserialize_attr_id(fvValue(values[1]), attrId);

    sai_attr_capability_t capability;

    sai_status_t status = m_vendorSai->queryAttributeCapability(switchRid, objectType, attrId, &capability);

    std::vector<swss::FieldValueTuple> entry;

    if (status == SAI_STATUS_SUCCESS)
    {
        entry =
        {
            swss::FieldValueTuple("CREATE_IMPLEMENTED", (capability.create_implemented ? "true" : "false")),
            swss::FieldValueTuple("SET_IMPLEMENTED",    (capability.set_implemented    ? "true" : "false")),
            swss::FieldValueTuple("GET_IMPLEMENTED",    (capability.get_implemented    ? "true" : "false"))
        };

        SWSS_LOG_INFO("Sending response: create_implemented:%d, set_implemented:%d, get_implemented:%d",
            capability.create_implemented, capability.set_implemented, capability.get_implemented);
    }

    m_selectableChannel->set(sai_serialize_status(status), entry, REDIS_ASIC_STATE_COMMAND_ATTR_CAPABILITY_RESPONSE);

    return status;
}

sai_status_t Syncd::processAttrEnumValuesCapabilityQuery(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    auto& strSwitchVid = kfvKey(kco);

    sai_object_id_t switchVid;
    sai_deserialize_object_id(strSwitchVid, switchVid);

    sai_object_id_t switchRid = m_translator->translateVidToRid(switchVid);

    auto& values = kfvFieldsValues(kco);

    if (values.size() != 3)
    {
        SWSS_LOG_ERROR("Invalid input: expected 3 arguments, received %zu", values.size());

        m_selectableChannel->set(sai_serialize_status(SAI_STATUS_INVALID_PARAMETER), {}, REDIS_ASIC_STATE_COMMAND_ATTR_ENUM_VALUES_CAPABILITY_RESPONSE);

        return SAI_STATUS_INVALID_PARAMETER;
    }

    sai_object_type_t objectType;
    sai_deserialize_object_type(fvValue(values[0]), objectType);

    sai_attr_id_t attrId;
    sai_deserialize_attr_id(fvValue(values[1]), attrId);

    uint32_t list_size = std::stoi(fvValue(values[2]));

    std::vector<int32_t> enum_capabilities_list(list_size);

    sai_s32_list_t enumCapList;

    enumCapList.count = list_size;
    enumCapList.list = enum_capabilities_list.data();

    sai_status_t status = m_vendorSai->queryAattributeEnumValuesCapability(switchRid, objectType, attrId, &enumCapList);

    std::vector<swss::FieldValueTuple> entry;

    if (status == SAI_STATUS_SUCCESS)
    {
        std::vector<std::string> vec;
        std::transform(enumCapList.list, enumCapList.list + enumCapList.count,
                std::back_inserter(vec), [](auto&e) { return std::to_string(e); });

        std::ostringstream join;
        std::copy(vec.begin(), vec.end(), std::ostream_iterator<std::string>(join, ","));

        auto strCap = join.str();

        entry =
        {
            swss::FieldValueTuple("ENUM_CAPABILITIES", strCap),
            swss::FieldValueTuple("ENUM_COUNT", std::to_string(enumCapList.count))
        };

        SWSS_LOG_DEBUG("Sending response: capabilities = '%s', count = %d", strCap.c_str(), enumCapList.count);
    }

    m_selectableChannel->set(sai_serialize_status(status), entry, REDIS_ASIC_STATE_COMMAND_ATTR_ENUM_VALUES_CAPABILITY_RESPONSE);

    return status;
}

sai_status_t Syncd::processObjectTypeGetAvailabilityQuery(
    _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    auto& strSwitchVid = kfvKey(kco);

    sai_object_id_t switchVid;
    sai_deserialize_object_id(strSwitchVid, switchVid);

    const sai_object_id_t switchRid = m_translator->translateVidToRid(switchVid);

    std::vector<swss::FieldValueTuple> values = kfvFieldsValues(kco);

    // Syncd needs to pop the object type off the end of the list in order to
    // retrieve the attribute list

    sai_object_type_t objectType;
    sai_deserialize_object_type(fvValue(values.back()), objectType);

    values.pop_back();

    SaiAttributeList list(objectType, values, false);

    sai_attribute_t *attr_list = list.get_attr_list();

    uint32_t attr_count = list.get_attr_count();

    m_translator->translateVidToRid(objectType, attr_count, attr_list);

    uint64_t count;

    sai_status_t status = m_vendorSai->objectTypeGetAvailability(
            switchRid,
            objectType,
            attr_count,
            attr_list,
            &count);

    std::vector<swss::FieldValueTuple> entry;

    if (status == SAI_STATUS_SUCCESS)
    {
        entry.push_back(swss::FieldValueTuple("OBJECT_COUNT", std::to_string(count)));

        SWSS_LOG_DEBUG("Sending response: count = %lu", count);
    }

    m_selectableChannel->set(sai_serialize_status(status), entry, REDIS_ASIC_STATE_COMMAND_OBJECT_TYPE_GET_AVAILABILITY_RESPONSE);

    return status;
}

sai_status_t Syncd::processFdbFlush(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    auto& key = kfvKey(kco);
    auto strSwitchVid = key.substr(key.find(":") + 1);

    sai_object_id_t switchVid;
    sai_deserialize_object_id(strSwitchVid, switchVid);

    sai_object_id_t switchRid = m_translator->translateVidToRid(switchVid);

    auto& values = kfvFieldsValues(kco);

    for (const auto &v: values)
    {
        SWSS_LOG_DEBUG("attr: %s: %s", fvField(v).c_str(), fvValue(v).c_str());
    }

    SaiAttributeList list(SAI_OBJECT_TYPE_FDB_FLUSH, values, false);
    SaiAttributeList vidlist(SAI_OBJECT_TYPE_FDB_FLUSH, values, false);

    /*
     * Attribute list can't be const since we will use it to translate VID to
     * RID in place.
     */

    sai_attribute_t *attr_list = list.get_attr_list();
    uint32_t attr_count = list.get_attr_count();

    m_translator->translateVidToRid(SAI_OBJECT_TYPE_FDB_FLUSH, attr_count, attr_list);

    sai_status_t status = m_vendorSai->flushFdbEntries(switchRid, attr_count, attr_list);

    m_selectableChannel->set(sai_serialize_status(status), {} , REDIS_ASIC_STATE_COMMAND_FLUSHRESPONSE);

    if (status == SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_NOTICE("fdb flush succeeded, updating redis database");

        // update database right after fdb flush success (not in notification)
        // build artificial notification here to reuse code

        sai_fdb_flush_entry_type_t type = SAI_FDB_FLUSH_ENTRY_TYPE_DYNAMIC;

        sai_object_id_t bvId = SAI_NULL_OBJECT_ID;
        sai_object_id_t bridgePortId = SAI_NULL_OBJECT_ID;

        attr_list = vidlist.get_attr_list();
        attr_count = vidlist.get_attr_count();

        for (uint32_t i = 0; i < attr_count; i++)
        {
            switch (attr_list[i].id)
            {
                case SAI_FDB_FLUSH_ATTR_BRIDGE_PORT_ID:
                    bridgePortId = attr_list[i].value.oid;
                    break;

                case SAI_FDB_FLUSH_ATTR_BV_ID:
                    bvId = attr_list[i].value.oid;
                    break;

                case SAI_FDB_FLUSH_ATTR_ENTRY_TYPE:
                    type = (sai_fdb_flush_entry_type_t)attr_list[i].value.s32;
                    break;

                default:
                    SWSS_LOG_ERROR("unsupported attribute: %d, skipping", attr_list[i].id);
                    break;
            }
        }

        m_client->processFlushEvent(switchVid, bridgePortId, bvId, type);
    }

    return status;
}

sai_status_t Syncd::processClearStatsEvent(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    const std::string &key = kfvKey(kco);

    sai_object_meta_key_t metaKey;
    sai_deserialize_object_meta_key(key, metaKey);

    if (!m_translator->tryTranslateVidToRid(metaKey))
    {
        SWSS_LOG_WARN("VID to RID translation failure: %s", key.c_str());
        sai_status_t status = SAI_STATUS_INVALID_OBJECT_ID;
        m_selectableChannel->set(sai_serialize_status(status), {}, REDIS_ASIC_STATE_COMMAND_GETRESPONSE);
        return status;
    }

    auto info = sai_metadata_get_object_type_info(metaKey.objecttype);

    if (info->isnonobjectid)
    {
        SWSS_LOG_THROW("non object id not supported on clear stats: %s, FIXME", key.c_str());
    }

    std::vector<sai_stat_id_t> counter_ids;

    for (auto&v: kfvFieldsValues(kco))
    {
        int32_t val;
        sai_deserialize_enum(fvField(v), info->statenum, val);

        counter_ids.push_back(val);
    }

    auto status = m_vendorSai->clearStats(
            metaKey.objecttype,
            metaKey.objectkey.key.object_id,
            (uint32_t)counter_ids.size(),
            counter_ids.data());

    m_selectableChannel->set(sai_serialize_status(status), {}, REDIS_ASIC_STATE_COMMAND_GETRESPONSE);

    return status;
}

sai_status_t Syncd::processGetStatsEvent(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    const std::string &key = kfvKey(kco);

    sai_object_meta_key_t metaKey;
    sai_deserialize_object_meta_key(key, metaKey);

    // TODO get stats on created object in init view mode could fail

    m_translator->translateVidToRid(metaKey);

    auto info = sai_metadata_get_object_type_info(metaKey.objecttype);

    if (info->isnonobjectid)
    {
        SWSS_LOG_THROW("non object id not supported on clear stats: %s, FIXME", key.c_str());
    }

    std::vector<sai_stat_id_t> counter_ids;

    for (auto&v: kfvFieldsValues(kco))
    {
        int32_t val;
        sai_deserialize_enum(fvField(v), info->statenum, val);

        counter_ids.push_back(val);
    }

    std::vector<uint64_t> result(counter_ids.size());

    auto status = m_vendorSai->getStats(
            metaKey.objecttype,
            metaKey.objectkey.key.object_id,
            (uint32_t)counter_ids.size(),
            counter_ids.data(),
            result.data());

    std::vector<swss::FieldValueTuple> entry;

    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_ERROR("Failed to get stats");
    }
    else
    {
        const auto& values = kfvFieldsValues(kco);

        for (size_t i = 0; i < values.size(); i++)
        {
            entry.emplace_back(fvField(values[i]), std::to_string(result[i]));
        }
    }

    m_selectableChannel->set(sai_serialize_status(status), entry, REDIS_ASIC_STATE_COMMAND_GETRESPONSE);

    return status;
}

sai_status_t Syncd::processBulkQuadEvent(
        _In_ sai_common_api_t api,
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    const std::string& key = kfvKey(kco); // objectType:count

    std::string strObjectType = key.substr(0, key.find(":"));

    sai_object_type_t objectType;
    sai_deserialize_object_type(strObjectType, objectType);

    const std::vector<swss::FieldValueTuple> &values = kfvFieldsValues(kco);

    std::vector<std::vector<swss::FieldValueTuple>> strAttributes;

    // field = objectId
    // value = attrid=attrvalue|...

    std::vector<std::string> objectIds;

    std::vector<std::shared_ptr<SaiAttributeList>> attributes;

    for (const auto &fvt: values)
    {
        std::string strObjectId = fvField(fvt);
        std::string joined = fvValue(fvt);

        // decode values

        auto v = swss::tokenize(joined, '|');

        objectIds.push_back(strObjectId);

        std::vector<swss::FieldValueTuple> entries; // attributes per object id

        for (size_t i = 0; i < v.size(); ++i)
        {
            const std::string item = v.at(i);

            auto start = item.find_first_of("=");

            auto field = item.substr(0, start);
            auto value = item.substr(start + 1);

            entries.emplace_back(field, value);
        }

        strAttributes.push_back(entries);

        // since now we converted this to proper list, we can extract attributes

        auto list = std::make_shared<SaiAttributeList>(objectType, entries, false);

        attributes.push_back(list);
    }

    SWSS_LOG_INFO("bulk %s executing with %zu items",
            strObjectType.c_str(),
            objectIds.size());

    if (isInitViewMode())
    {
        return processBulkQuadEventInInitViewMode(objectType, objectIds, api, attributes, strAttributes);
    }

    if (api != SAI_COMMON_API_BULK_GET)
    {
        // translate attributes for all objects

        for (auto &list: attributes)
        {
            sai_attribute_t *attr_list = list->get_attr_list();
            uint32_t attr_count = list->get_attr_count();

            m_translator->translateVidToRid(objectType, attr_count, attr_list);
        }
    }

    auto info = sai_metadata_get_object_type_info(objectType);

    if (info->isobjectid)
    {
        return processBulkOid(objectType, objectIds, api, attributes, strAttributes);
    }
    else
    {
        return processBulkEntry(objectType, objectIds, api, attributes, strAttributes);
    }
}

sai_status_t Syncd::processBulkQuadEventInInitViewMode(
        _In_ sai_object_type_t objectType,
        _In_ const std::vector<std::string>& objectIds,
        _In_ sai_common_api_t api,
        _In_ const std::vector<std::shared_ptr<saimeta::SaiAttributeList>>& attributes,
        _In_ const std::vector<std::vector<swss::FieldValueTuple>>& strAttributes)
{
    SWSS_LOG_ENTER();

    std::vector<sai_status_t> statuses(objectIds.size());

    for (auto &a: statuses)
    {
        a = SAI_STATUS_SUCCESS;
    }

    auto info = sai_metadata_get_object_type_info(objectType);

    switch (api)
    {
        case SAI_COMMON_API_BULK_CREATE:
        case SAI_COMMON_API_BULK_REMOVE:
        case SAI_COMMON_API_BULK_SET:

            if (info->isnonobjectid)
            {
                sendApiResponse(api, SAI_STATUS_SUCCESS, (uint32_t)statuses.size(), statuses.data());

                syncUpdateRedisBulkQuadEvent(api, statuses, objectType, objectIds, strAttributes);

                return SAI_STATUS_SUCCESS;
            }

            switch (objectType)
            {
                case SAI_OBJECT_TYPE_SWITCH:
                case SAI_OBJECT_TYPE_PORT:
                case SAI_OBJECT_TYPE_SCHEDULER_GROUP:
                case SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP:

                    SWSS_LOG_THROW("%s is not supported in init view mode",
                            sai_serialize_object_type(objectType).c_str());

                default:

                    sendApiResponse(api, SAI_STATUS_SUCCESS, (uint32_t)statuses.size(), statuses.data());

                    syncUpdateRedisBulkQuadEvent(api, statuses, objectType, objectIds, strAttributes);

                    return SAI_STATUS_SUCCESS;
            }

        case SAI_COMMON_API_BULK_GET:
            SWSS_LOG_THROW("GET bulk api is not implemented in init view mode, FIXME");

        default:

            SWSS_LOG_THROW("common bulk api (%s) is not implemented in init view mode",
                    sai_serialize_common_api(api).c_str());
    }
}

sai_status_t Syncd::processBulkCreateEntry(
        _In_ sai_object_type_t objectType,
        _In_ const std::vector<std::string>& objectIds,
        _In_ const std::vector<std::shared_ptr<SaiAttributeList>>& attributes,
        _Out_ std::vector<sai_status_t>& statuses)
{
    SWSS_LOG_ENTER();
    sai_status_t status = SAI_STATUS_SUCCESS;

    uint32_t object_count = (uint32_t) objectIds.size();

    if (!object_count)
    {
        SWSS_LOG_ERROR("container with objectIds is empty in processBulkCreateEntry");
        return SAI_STATUS_FAILURE;
    }

    sai_bulk_op_error_mode_t mode = SAI_BULK_OP_ERROR_MODE_IGNORE_ERROR;

    std::vector<uint32_t> attr_counts(object_count);
    std::vector<const sai_attribute_t*> attr_lists(object_count);

    for (uint32_t idx = 0; idx < object_count; idx++)
    {
        attr_counts[idx] = attributes[idx]->get_attr_count();
        attr_lists[idx] = attributes[idx]->get_attr_list();
    }

    switch (objectType)
    {
        case SAI_OBJECT_TYPE_ROUTE_ENTRY:
        {
            std::vector<sai_route_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_route_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].vr_id = m_translator->translateVidToRid(entries[it].vr_id);
            }

            static PerformanceIntervalTimer timer("Syncd::processBulkCreateEntry(route_entry) CREATE");

            timer.start();

            status = m_vendorSai->bulkCreate(
                    object_count,
                    entries.data(),
                    attr_counts.data(),
                    attr_lists.data(),
                    mode,
                    statuses.data());

            timer.stop();

            timer.inc(object_count);
        }
        break;

        case SAI_OBJECT_TYPE_FDB_ENTRY:
        {
            std::vector<sai_fdb_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_fdb_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].bv_id = m_translator->translateVidToRid(entries[it].bv_id);
            }

            status = m_vendorSai->bulkCreate(
                    object_count,
                    entries.data(),
                    attr_counts.data(),
                    attr_lists.data(),
                    mode,
                    statuses.data());

        }
        break;

        case SAI_OBJECT_TYPE_NAT_ENTRY:
        {
            std::vector<sai_nat_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_nat_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].vr_id = m_translator->translateVidToRid(entries[it].vr_id);
            }

            status = m_vendorSai->bulkCreate(
                    object_count,
                    entries.data(),
                    attr_counts.data(),
                    attr_lists.data(),
                    mode,
                    statuses.data());

        }
        break;

        case SAI_OBJECT_TYPE_INSEG_ENTRY:
        {
            std::vector<sai_inseg_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_inseg_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
            }

            status = m_vendorSai->bulkCreate(
                    object_count,
                    entries.data(),
                    attr_counts.data(),
                    attr_lists.data(),
                    mode,
                    statuses.data());

        }
        break;

        default:
            return SAI_STATUS_NOT_SUPPORTED;
    }

    return status;
}

sai_status_t Syncd::processBulkRemoveEntry(
        _In_ sai_object_type_t objectType,
        _In_ const std::vector<std::string>& objectIds,
        _Out_ std::vector<sai_status_t>& statuses)
{
    SWSS_LOG_ENTER();

    sai_status_t status = SAI_STATUS_SUCCESS;

    uint32_t object_count = (uint32_t) objectIds.size();

    if (!object_count)
    {
        SWSS_LOG_ERROR("container with objectIds is empty in processBulkRemoveEntry");
        return SAI_STATUS_FAILURE;
    }

    sai_bulk_op_error_mode_t mode = SAI_BULK_OP_ERROR_MODE_IGNORE_ERROR;

    switch (objectType)
    {
        case SAI_OBJECT_TYPE_ROUTE_ENTRY:
        {
            std::vector<sai_route_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_route_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].vr_id = m_translator->translateVidToRid(entries[it].vr_id);
            }

            status = m_vendorSai->bulkRemove(
                    object_count,
                    entries.data(),
                    mode,
                    statuses.data());

        }
        break;

        case SAI_OBJECT_TYPE_FDB_ENTRY:
        {
            std::vector<sai_fdb_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_fdb_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].bv_id = m_translator->translateVidToRid(entries[it].bv_id);
            }

            status = m_vendorSai->bulkRemove(
                    object_count,
                    entries.data(),
                    mode,
                    statuses.data());

        }
        break;

        case SAI_OBJECT_TYPE_NAT_ENTRY:
        {
            std::vector<sai_nat_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_nat_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].vr_id = m_translator->translateVidToRid(entries[it].vr_id);
            }

            status = m_vendorSai->bulkRemove(
                    object_count,
                    entries.data(),
                    mode,
                    statuses.data());

        }
        break;

        case SAI_OBJECT_TYPE_INSEG_ENTRY:
        {
            std::vector<sai_inseg_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_inseg_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
            }

            status = m_vendorSai->bulkRemove(
                    object_count,
                    entries.data(),
                    mode,
                    statuses.data());

        }
        break;

        default:
            return SAI_STATUS_NOT_SUPPORTED;
    }

    return status;
}

sai_status_t Syncd::processBulkSetEntry(
        _In_ sai_object_type_t objectType,
        _In_ const std::vector<std::string>& objectIds,
        _In_ const std::vector<std::shared_ptr<SaiAttributeList>>& attributes,
        _Out_ std::vector<sai_status_t>& statuses)
{
    SWSS_LOG_ENTER();

    sai_status_t status = SAI_STATUS_SUCCESS;

    std::vector<sai_attribute_t> attr_lists;

    uint32_t object_count = (uint32_t) objectIds.size();

    if (!object_count)
    {
        SWSS_LOG_ERROR("container with objectIds is empty in processBulkSetEntry");
        return SAI_STATUS_FAILURE;
    }

    sai_bulk_op_error_mode_t mode = SAI_BULK_OP_ERROR_MODE_IGNORE_ERROR;

    for (uint32_t it = 0; it < object_count; it++)
    {
        attr_lists.push_back(attributes[it]->get_attr_list()[0]);
    }

    switch (objectType)
    {
        case SAI_OBJECT_TYPE_ROUTE_ENTRY:
        {
            std::vector<sai_route_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_route_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].vr_id = m_translator->translateVidToRid(entries[it].vr_id);
            }

            status = m_vendorSai->bulkSet(
                    object_count,
                    entries.data(),
                    attr_lists.data(),
                    mode,
                    statuses.data());

        }
        break;

        case SAI_OBJECT_TYPE_FDB_ENTRY:
        {
            std::vector<sai_fdb_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_fdb_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].bv_id = m_translator->translateVidToRid(entries[it].bv_id);
            }

            status = m_vendorSai->bulkSet(
                    object_count,
                    entries.data(),
                    attr_lists.data(),
                    mode,
                    statuses.data());

        }
        break;

        case SAI_OBJECT_TYPE_NAT_ENTRY:
        {
            std::vector<sai_nat_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_nat_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
                entries[it].vr_id = m_translator->translateVidToRid(entries[it].vr_id);
            }

            status = m_vendorSai->bulkSet(
                    object_count,
                    entries.data(),
                    attr_lists.data(),
                    mode,
                    statuses.data());

        }
        break;

        case SAI_OBJECT_TYPE_INSEG_ENTRY:
        {
            std::vector<sai_inseg_entry_t> entries(object_count);
            for (uint32_t it = 0; it < object_count; it++)
            {
                sai_deserialize_inseg_entry(objectIds[it], entries[it]);

                entries[it].switch_id = m_translator->translateVidToRid(entries[it].switch_id);
            }

            status = m_vendorSai->bulkSet(
                    object_count,
                    entries.data(),
                    attr_lists.data(),
                    mode,
                    statuses.data());

        }
        break;

        default:
            return SAI_STATUS_NOT_SUPPORTED;
    }

    return status;
}

sai_status_t Syncd::processBulkEntry(
        _In_ sai_object_type_t objectType,
        _In_ const std::vector<std::string>& objectIds,
        _In_ sai_common_api_t api,
        _In_ const std::vector<std::shared_ptr<SaiAttributeList>>& attributes,
        _In_ const std::vector<std::vector<swss::FieldValueTuple>>& strAttributes)
{
    SWSS_LOG_ENTER();

    auto info = sai_metadata_get_object_type_info(objectType);

    if (info->isobjectid)
    {
        SWSS_LOG_THROW("passing oid object to bulk non object id operation");
    }

    std::vector<sai_status_t> statuses(objectIds.size());

    sai_status_t all = SAI_STATUS_SUCCESS;

    if (m_commandLineOptions->m_enableSaiBulkSupport)
    {
        switch (api)
        {
            case SAI_COMMON_API_BULK_CREATE:
                all = processBulkCreateEntry(objectType, objectIds, attributes, statuses);
                break;

            case SAI_COMMON_API_BULK_REMOVE:
                all = processBulkRemoveEntry(objectType, objectIds, statuses);
                break;

            case SAI_COMMON_API_BULK_SET:
                all = processBulkSetEntry(objectType, objectIds, attributes, statuses);
                break;

            default:
                SWSS_LOG_ERROR("api %s is not supported in bulk", sai_serialize_common_api(api).c_str());
                all = SAI_STATUS_NOT_SUPPORTED;
        }

        if (all != SAI_STATUS_NOT_SUPPORTED && all != SAI_STATUS_NOT_IMPLEMENTED)
        {
            sendApiResponse(api, all, (uint32_t)objectIds.size(), statuses.data());
            syncUpdateRedisBulkQuadEvent(api, statuses, objectType, objectIds, strAttributes);

            return all;
        }
    }

    // vendor SAI don't bulk API yet, so execute one by one

    for (size_t idx = 0; idx < objectIds.size(); ++idx)
    {
        sai_object_meta_key_t metaKey;

        metaKey.objecttype = objectType;

        switch (objectType)
        {
            case SAI_OBJECT_TYPE_ROUTE_ENTRY:
                sai_deserialize_route_entry(objectIds[idx], metaKey.objectkey.key.route_entry);
                break;

            case SAI_OBJECT_TYPE_FDB_ENTRY:
                sai_deserialize_fdb_entry(objectIds[idx], metaKey.objectkey.key.fdb_entry);
                break;

            case SAI_OBJECT_TYPE_INSEG_ENTRY:
                sai_deserialize_inseg_entry(objectIds[idx], metaKey.objectkey.key.inseg_entry);
                break;

            default:
                SWSS_LOG_THROW("object %s not implemented, FIXME", sai_serialize_object_type(objectType).c_str());
        }

        sai_status_t status = SAI_STATUS_FAILURE;

        auto& list = attributes[idx];

        sai_attribute_t *attr_list = list->get_attr_list();
        uint32_t attr_count = list->get_attr_count();

        if (api == SAI_COMMON_API_BULK_CREATE)
        {
            if (objectType == SAI_OBJECT_TYPE_ROUTE_ENTRY)
            {
                static PerformanceIntervalTimer timer("Syncd::processBulkEntry::processEntry(route_entry) CREATE");

                timer.start();

                status = processEntry(metaKey, SAI_COMMON_API_CREATE, attr_count, attr_list);

                timer.stop();

                timer.inc();
            }
            else
            {
                status = processEntry(metaKey, SAI_COMMON_API_CREATE, attr_count, attr_list);
            }
        }
        else if (api == SAI_COMMON_API_BULK_REMOVE)
        {
            status = processEntry(metaKey, SAI_COMMON_API_REMOVE, attr_count, attr_list);
        }
        else if (api == SAI_COMMON_API_BULK_SET)
        {
            status = processEntry(metaKey, SAI_COMMON_API_SET, attr_count, attr_list);
        }
        else
        {
            SWSS_LOG_THROW("api %d is not supported in bulk mode", api);
        }

        if (api != SAI_COMMON_API_BULK_GET && status != SAI_STATUS_SUCCESS)
        {
            if (!m_enableSyncMode)
            {
                SWSS_LOG_THROW("operation %s for %s failed in async mode!",
                        sai_serialize_common_api(api).c_str(),
                        sai_serialize_object_type(objectType).c_str());
            }

            all = SAI_STATUS_FAILURE; // all can be success if all has been success
        }

        statuses[idx] = status;
    }

    sendApiResponse(api, all, (uint32_t)objectIds.size(), statuses.data());

    syncUpdateRedisBulkQuadEvent(api, statuses, objectType, objectIds, strAttributes);

    return all;
}

sai_status_t Syncd::processEntry(
        _In_ sai_object_meta_key_t metaKey,
        _In_ sai_common_api_t api,
        _In_ uint32_t attr_count,
        _In_ sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    m_translator->translateVidToRid(metaKey);

    switch (api)
    {
        case SAI_COMMON_API_CREATE:
            return m_vendorSai->create(metaKey, SAI_NULL_OBJECT_ID, attr_count, attr_list);

        case SAI_COMMON_API_REMOVE:
            return m_vendorSai->remove(metaKey);

        case SAI_COMMON_API_SET:
            return m_vendorSai->set(metaKey, attr_list);

        case SAI_COMMON_API_GET:
            return m_vendorSai->get(metaKey, attr_count, attr_list);

        default:

            SWSS_LOG_THROW("api %s not supported", sai_serialize_common_api(api).c_str());
    }
}

sai_status_t Syncd::processBulkOidCreate(
        _In_ sai_object_type_t objectType,
        _In_ sai_bulk_op_error_mode_t mode,
        _In_ const std::vector<std::string>& objectIds,
        _In_ const std::vector<std::shared_ptr<saimeta::SaiAttributeList>>& attributes,
        _Out_ std::vector<sai_status_t>& statuses)
{
    SWSS_LOG_ENTER();

    sai_status_t status = SAI_STATUS_SUCCESS;
    uint32_t object_count = (uint32_t)objectIds.size();

    if (!object_count)
    {
        SWSS_LOG_ERROR("container with objectIds is empty in processBulkOidCreate");
        return SAI_STATUS_FAILURE;
    }

    std::vector<sai_object_id_t> objectVids(object_count);

    std::vector<uint32_t> attr_counts(object_count);
    std::vector<const sai_attribute_t*> attr_lists(object_count);

    for (size_t idx = 0; idx < object_count; idx++)
    {
        sai_deserialize_object_id(objectIds[idx], objectVids[idx]);

        attr_counts[idx] = attributes[idx]->get_attr_count();
        attr_lists[idx] = attributes[idx]->get_attr_list();
    }

    sai_object_id_t switchRid = SAI_NULL_OBJECT_ID;

    sai_object_id_t switchVid = VidManager::switchIdQuery(objectVids.front());
    switchRid = m_translator->translateVidToRid(switchVid);


    std::vector<sai_object_id_t> objectRids(object_count);

    status = m_vendorSai->bulkCreate(
                                objectType,
                                switchRid,
                                object_count,
                                attr_counts.data(),
                                attr_lists.data(),
                                mode,
                                objectRids.data(),
                                statuses.data());

    if (status == SAI_STATUS_NOT_IMPLEMENTED || status == SAI_STATUS_NOT_SUPPORTED)
    {
        SWSS_LOG_ERROR("bulkCreate api is not implemented or not supported, object_type = %s",
                sai_serialize_object_type(objectType).c_str());
        return status;
    }

    /*
     * Object was created so new object id was generated we need to save
     * virtual id's to redis db.
     */
    for (size_t idx = 0; idx < object_count; idx++)
    {
        if (statuses[idx] == SAI_STATUS_SUCCESS)
        {
            m_translator->insertRidAndVid(objectRids[idx], objectVids[idx]);
            SWSS_LOG_INFO("saved VID %s to RID %s",
                    sai_serialize_object_id(objectVids[idx]).c_str(),
                    sai_serialize_object_id(objectRids[idx]).c_str());
        }
    }

    return status;
}

sai_status_t Syncd::processBulkOidRemove(
        _In_ sai_object_type_t objectType,
        _In_ sai_bulk_op_error_mode_t mode,
        _In_ const std::vector<std::string>& objectIds,
        _Out_ std::vector<sai_status_t>& statuses)
{
    SWSS_LOG_ENTER();

    sai_status_t status = SAI_STATUS_SUCCESS;
    uint32_t object_count = (uint32_t)objectIds.size();

    if (!object_count)
    {
        SWSS_LOG_ERROR("container with objectIds is empty in processBulkOidRemove");
        return SAI_STATUS_FAILURE;
    }

    std::vector<sai_object_id_t> objectVids(object_count);
    std::vector<sai_object_id_t> objectRids(object_count);

    for (size_t idx = 0; idx < object_count; idx++)
    {
        sai_deserialize_object_id(objectIds[idx], objectVids[idx]);
        objectRids[idx] = m_translator->translateVidToRid(objectVids[idx]);
    }

    status = m_vendorSai->bulkRemove(
                                objectType,
                                (uint32_t)object_count,
                                objectRids.data(),
                                mode,
                                statuses.data());

    if (status == SAI_STATUS_NOT_IMPLEMENTED || status == SAI_STATUS_NOT_SUPPORTED)
    {
        SWSS_LOG_ERROR("bulkRemove api is not implemented or not supported, object_type = %s",
                sai_serialize_object_type(objectType).c_str());
        return status;
    }

    /*
     * remove all related objects from REDIS DB and also from existing
     * object references since at this point they are no longer valid
     */
    sai_object_id_t switchVid;
    for (size_t idx = 0; idx < object_count; idx++)
    {
        if (statuses[idx] == SAI_STATUS_SUCCESS)
        {
            m_translator->eraseRidAndVid(objectRids[idx], objectVids[idx]);

            switchVid = VidManager::switchIdQuery(objectVids[idx]);

            if (m_switches.at(switchVid)->isDiscoveredRid(objectRids[idx]))
            {
                m_switches.at(switchVid)->removeExistingObjectReference(objectRids[idx]);
            }
        }
    }

    return status;
}

sai_status_t Syncd::processBulkOid(
        _In_ sai_object_type_t objectType,
        _In_ const std::vector<std::string>& objectIds,
        _In_ sai_common_api_t api,
        _In_ const std::vector<std::shared_ptr<SaiAttributeList>>& attributes,
        _In_ const std::vector<std::vector<swss::FieldValueTuple>>& strAttributes)
{
    SWSS_LOG_ENTER();

    auto info = sai_metadata_get_object_type_info(objectType);

    if (info->isnonobjectid)
    {
        SWSS_LOG_THROW("passing non object id to bulk oid object operation");
    }

    std::vector<sai_status_t> statuses(objectIds.size());

    sai_status_t all = SAI_STATUS_SUCCESS;

    if (m_commandLineOptions->m_enableSaiBulkSupport)
    {
        sai_bulk_op_error_mode_t mode = SAI_BULK_OP_ERROR_MODE_IGNORE_ERROR;

        switch (api)
        {
            case SAI_COMMON_API_BULK_CREATE:
                all = processBulkOidCreate(objectType, mode, objectIds, attributes, statuses);
                break;

            case SAI_COMMON_API_BULK_REMOVE:
                all = processBulkOidRemove(objectType, mode, objectIds, statuses);
                break;

            default:
                all = SAI_STATUS_NOT_SUPPORTED;
                SWSS_LOG_ERROR("api %s is not supported in bulk mode", sai_serialize_common_api(api).c_str());
        }

        if (all != SAI_STATUS_NOT_SUPPORTED && all != SAI_STATUS_NOT_IMPLEMENTED)
        {
            sendApiResponse(api, all, (uint32_t)objectIds.size(), statuses.data());
            syncUpdateRedisBulkQuadEvent(api, statuses, objectType, objectIds, strAttributes);

            return all;
        }
    }

    for (size_t idx = 0; idx < objectIds.size(); ++idx)
    {
        sai_status_t status = SAI_STATUS_FAILURE;

        auto& list = attributes[idx];

        sai_attribute_t *attr_list = list->get_attr_list();
        uint32_t attr_count = list->get_attr_count();

        if (api == SAI_COMMON_API_BULK_CREATE)
        {
            status = processOid(objectType, objectIds[idx], SAI_COMMON_API_CREATE, attr_count, attr_list);
        }
        else if (api == SAI_COMMON_API_BULK_REMOVE)
        {
            status = processOid(objectType, objectIds[idx], SAI_COMMON_API_REMOVE, attr_count, attr_list);
        }
        else if (api == SAI_COMMON_API_BULK_SET)
        {
            status = processOid(objectType, objectIds[idx], SAI_COMMON_API_SET, attr_count, attr_list);
        }
        else
        {
            SWSS_LOG_THROW("api %s is not supported in bulk mode",
                    sai_serialize_common_api(api).c_str());
        }

        if (status != SAI_STATUS_SUCCESS)
        {
            if (!m_enableSyncMode)
            {
                SWSS_LOG_THROW("operation %s for %s failed in async mode!",
                        sai_serialize_common_api(api).c_str(),
                        sai_serialize_object_type(objectType).c_str());
            }

            all = SAI_STATUS_FAILURE; // all can be success if all has been success
        }

        statuses[idx] = status;
    }

    sendApiResponse(api, all, (uint32_t)objectIds.size(), statuses.data());

    syncUpdateRedisBulkQuadEvent(api, statuses, objectType, objectIds, strAttributes);

    return all;
}

sai_status_t Syncd::processQuadEventInInitViewMode(
        _In_ sai_object_type_t objectType,
        _In_ const std::string& strObjectId,
        _In_ sai_common_api_t api,
        _In_ uint32_t attr_count,
        _In_ sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    /*
     * Since attributes are not checked, it may happen that user will send some
     * invalid VID in object id/list in attribute, metadata should handle that,
     * but if that happen, this id will be treated as "new" object instead of
     * existing one.
     */

    switch (api)
    {
        case SAI_COMMON_API_CREATE:
            return processQuadInInitViewModeCreate(objectType, strObjectId, attr_count, attr_list);

        case SAI_COMMON_API_REMOVE:
            return processQuadInInitViewModeRemove(objectType, strObjectId);

        case SAI_COMMON_API_SET:
            return processQuadInInitViewModeSet(objectType, strObjectId, attr_list);

        case SAI_COMMON_API_GET:
            return processQuadInInitViewModeGet(objectType, strObjectId, attr_count, attr_list);

        default:

            SWSS_LOG_THROW("common api (%s) is not implemented in init view mode", sai_serialize_common_api(api).c_str());
    }
}

sai_status_t Syncd::processQuadInInitViewModeCreate(
        _In_ sai_object_type_t objectType,
        _In_ const std::string& strObjectId,
        _In_ uint32_t attr_count,
        _In_ sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    if (objectType == SAI_OBJECT_TYPE_PORT)
    {
        /*
         * Reason for this is that if user will create port, new port is not
         * actually created so when for example querying new queues for new
         * created port, there are not there, since no actual port create was
         * issued on the ASIC.
         */

        SWSS_LOG_THROW("port object can't be created in init view mode");
    }

    auto info = sai_metadata_get_object_type_info(objectType);

    // we assume create of those non object id object types will succeed

    if (info->isobjectid)
    {
        sai_object_id_t objectVid;
        sai_deserialize_object_id(strObjectId, objectVid);

        /*
         * Object ID here is actual VID returned from redis during
         * creation this is floating VID in init view mode.
         */

        SWSS_LOG_DEBUG("generic create (init view) for %s, floating VID: %s",
                sai_serialize_object_type(objectType).c_str(),
                sai_serialize_object_id(objectVid).c_str());

        if (objectType == SAI_OBJECT_TYPE_SWITCH)
        {
            onSwitchCreateInInitViewMode(objectVid, attr_count, attr_list);
        }
    }

    sendApiResponse(SAI_COMMON_API_CREATE, SAI_STATUS_SUCCESS);

    return SAI_STATUS_SUCCESS;
}

sai_status_t Syncd::processQuadInInitViewModeRemove(
        _In_ sai_object_type_t objectType,
        _In_ const std::string& strObjectId)
{
    SWSS_LOG_ENTER();

    if (objectType == SAI_OBJECT_TYPE_PORT)
    {
        /*
         * Reason for this is that if user will remove port, actual resources
         * for it won't be released, lanes would be still occupied and there is
         * extra logic required in post port remove which clears OIDs
         * (ipgs,queues,SGs) from redis db that are automatically removed by
         * vendor SAI, and comparison logic don't support that.
         */

        SWSS_LOG_THROW("port object (%s) can't be removed in init view mode", strObjectId.c_str());
    }

    if (objectType == SAI_OBJECT_TYPE_SWITCH)
    {
        /*
         * NOTE: Special care needs to be taken to clear all this switch id's
         * from all db's currently we skip this since we assume that orchagent
         * will not be removing switches, just creating.  But it may happen
         * when asic will fail etc.
         *
         * To support multiple switches this case must be refactored.
         */

        SWSS_LOG_THROW("remove switch (%s) is not supported in init view mode yet! FIXME", strObjectId.c_str());
    }

    // NOTE: we should also prevent removing some other non removable objects

    auto info = sai_metadata_get_object_type_info(objectType);

    if (info->isobjectid)
    {
        /*
         * If object is existing object (like bridge port, vlan member) user
         * may want to remove them, but this is temporary view, and when we
         * receive apply view, we will populate existing objects to temporary
         * view (since not all of them user may query) and this will produce
         * conflict, since some of those objects user could explicitly remove.
         * So to solve that we need to have a list of removed objects, and then
         * only populate objects which not exist on removed list.
         */

        sai_object_id_t objectVid;
        sai_deserialize_object_id(strObjectId, objectVid);

        // this set may contain removed objects from multiple switches

        m_initViewRemovedVidSet.insert(objectVid);
    }

    sendApiResponse(SAI_COMMON_API_REMOVE, SAI_STATUS_SUCCESS);

    return SAI_STATUS_SUCCESS;
}

sai_status_t Syncd::processQuadInInitViewModeSet(
        _In_ sai_object_type_t objectType,
        _In_ const std::string& strObjectId,
        _In_ sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    // we support SET api on all objects in init view mode

    sendApiResponse(SAI_COMMON_API_SET, SAI_STATUS_SUCCESS);

    return SAI_STATUS_SUCCESS;
}

sai_status_t Syncd::processQuadInInitViewModeGet(
        _In_ sai_object_type_t objectType,
        _In_ const std::string& strObjectId,
        _In_ uint32_t attr_count,
        _In_ sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    sai_status_t status;

    auto info = sai_metadata_get_object_type_info(objectType);

    sai_object_id_t switchVid = SAI_NULL_OBJECT_ID;

    if (info->isnonobjectid)
    {
        /*
         * Those objects are user created, so if user created ROUTE he
         * passed some attributes, there is no sense to support GET
         * since user explicitly know what attributes were set, similar
         * for other non object id types.
         */

        SWSS_LOG_ERROR("get is not supported on %s in init view mode", sai_serialize_object_type(objectType).c_str());

        status = SAI_STATUS_NOT_SUPPORTED;
    }
    else
    {
        sai_object_id_t objectVid;
        sai_deserialize_object_id(strObjectId, objectVid);

        switchVid = VidManager::switchIdQuery(objectVid);

        SWSS_LOG_DEBUG("generic get (init view) for object type %s:%s",
                sai_serialize_object_type(objectType).c_str(),
                strObjectId.c_str());

        /*
         * Object must exists, we can't call GET on created object
         * in init view mode, get here can be called on existing
         * objects like default trap group to get some vendor
         * specific values.
         *
         * Exception here is switch, since all switches must be
         * created, when user will create switch on init view mode,
         * switch will be matched with existing switch, or it will
         * be explicitly created so user can query it properties.
         *
         * Translate vid to rid will make sure that object exist
         * and it have RID defined, so we can query it.
         */

        sai_object_id_t rid = m_translator->translateVidToRid(objectVid);

        sai_object_meta_key_t metaKey;

        metaKey.objecttype = objectType;
        metaKey.objectkey.key.object_id = rid;

        status = m_vendorSai->get(metaKey, attr_count, attr_list);
    }

    /*
     * We are in init view mode, but ether switch already existed or first
     * command was creating switch and user created switch.
     *
     * We could change that later on, depends on object type we can extract
     * switch id, we could also have this method inside metadata to get meta
     * key.
     */

    sendGetResponse(objectType, strObjectId, switchVid, status, attr_count, attr_list);

    return status;
}

void Syncd::sendApiResponse(
        _In_ sai_common_api_t api,
        _In_ sai_status_t status,
        _In_ uint32_t object_count,
        _In_ sai_status_t* object_statuses)
{
    SWSS_LOG_ENTER();

    /*
     * By default synchronous mode is disabled and can be enabled by command
     * line on syncd start. This will also require to enable synchronous mode
     * in OA/sairedis because same GET RESPONSE channel is used to generate
     * response for sairedis quad API.
     */

    if (!m_enableSyncMode)
    {
        return;
    }

    switch (api)
    {
        case SAI_COMMON_API_CREATE:
        case SAI_COMMON_API_REMOVE:
        case SAI_COMMON_API_SET:
        case SAI_COMMON_API_BULK_CREATE:
        case SAI_COMMON_API_BULK_REMOVE:
        case SAI_COMMON_API_BULK_SET:
            break;

        default:
            SWSS_LOG_THROW("api %s not supported by this function",
                    sai_serialize_common_api(api).c_str());
    }

    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_ERROR("api %s failed in syncd mode: %s",
                    sai_serialize_common_api(api).c_str(),
                    sai_serialize_status(status).c_str());
    }

    std::vector<swss::FieldValueTuple> entry;

    for (uint32_t idx = 0; idx < object_count; idx++)
    {
        swss::FieldValueTuple fvt(sai_serialize_status(object_statuses[idx]), "");

        entry.push_back(fvt);
    }

    std::string strStatus = sai_serialize_status(status);

    SWSS_LOG_INFO("sending response for %s api with status: %s",
            sai_serialize_common_api(api).c_str(),
            strStatus.c_str());

    m_selectableChannel->set(strStatus, entry, REDIS_ASIC_STATE_COMMAND_GETRESPONSE);

    SWSS_LOG_INFO("response for %s api was send",
            sai_serialize_common_api(api).c_str());
}

void Syncd::processFlexCounterGroupEvent( // TODO must be moved to go via ASIC channel queue
        _In_ swss::ConsumerTable& consumer)
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    swss::KeyOpFieldsValuesTuple kco;

    consumer.pop(kco);

    auto& groupName = kfvKey(kco);
    auto& op = kfvOp(kco);
    auto& values = kfvFieldsValues(kco);

    if (op == SET_COMMAND)
    {
        m_manager->addCounterPlugin(groupName, values);
    }
    else if (op == DEL_COMMAND)
    {
        m_manager->removeCounterPlugins(groupName);
    }
    else
    {
        SWSS_LOG_ERROR("unknown command: %s", op.c_str());
    }
}

void Syncd::processFlexCounterEvent( // TODO must be moved to go via ASIC channel queue
        _In_ swss::ConsumerTable& consumer)
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    swss::KeyOpFieldsValuesTuple kco;

    consumer.pop(kco);

    auto& key = kfvKey(kco);
    auto& op = kfvOp(kco);

    auto delimiter = key.find_first_of(":");

    if (delimiter == std::string::npos)
    {
        SWSS_LOG_ERROR("Failed to parse the key %s", key.c_str());

        return; // if key is invalid there is no need to process this event again
    }

    auto groupName = key.substr(0, delimiter);
    auto strVid = key.substr(delimiter + 1);

    sai_object_id_t vid;
    sai_deserialize_object_id(strVid, vid);

    sai_object_id_t rid;

    if (!m_translator->tryTranslateVidToRid(vid, rid))
    {
        SWSS_LOG_WARN("port VID %s, was not found (probably port was removed/splitted) and will remove from counters now",
                sai_serialize_object_id(vid).c_str());

        op = DEL_COMMAND;
    }

    const auto values = kfvFieldsValues(kco);

    if (op == SET_COMMAND)
    {
        m_manager->addCounter(vid, rid, groupName, values);
    }
    else if (op == DEL_COMMAND)
    {
        m_manager->removeCounter(vid, groupName);
    }
    else
    {
        SWSS_LOG_ERROR("unknown command: %s", op.c_str());
    }
}

void Syncd::syncUpdateRedisQuadEvent(
        _In_ sai_status_t status,
        _In_ sai_common_api_t api,
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    if (!m_enableSyncMode)
    {
        return;
    }

    if (status != SAI_STATUS_SUCCESS)
    {
        return;
    }

    // When in synchronous mode, we need to modify redis database when status
    // is success, since consumer table on synchronous mode is not making redis
    // changes and we only want to apply changes when api succeeded. This
    // applies to init view mode and apply view mode.

    const std::string& key = kfvKey(kco);

    auto& values = kfvFieldsValues(kco);

    sai_object_meta_key_t metaKey;
    sai_deserialize_object_meta_key(key, metaKey);

    const bool initView = isInitViewMode();

    static PerformanceIntervalTimer timer("Syncd::syncUpdateRedisQuadEvent");

    timer.start();

    switch (api)
    {
        case SAI_COMMON_API_CREATE:

            {
                if (initView)
                    m_client->createTempAsicObject(metaKey, values);
                else
                    m_client->createAsicObject(metaKey, values);

                break;
            }

        case SAI_COMMON_API_REMOVE:

            {
                if (initView)
                    m_client->removeTempAsicObject(metaKey);
                else
                    m_client->removeAsicObject(metaKey);

                break;
            }

        case SAI_COMMON_API_SET:

            {
                auto& first = values.at(0);

                auto& attr = fvField(first);
                auto& value = fvValue(first);

                if (initView)
                    m_client->setTempAsicObject(metaKey, attr, value);
                else
                    m_client->setAsicObject(metaKey, attr, value);

                break;
            }

        case SAI_COMMON_API_GET:
            break; // ignore get since get is not modifying db

        default:

            SWSS_LOG_THROW("api %d is not supported", api);
    }

    timer.stop();

    timer.inc();
}

void Syncd::syncUpdateRedisBulkQuadEvent(
        _In_ sai_common_api_t api,
        _In_ const std::vector<sai_status_t>& statuses,
        _In_ sai_object_type_t objectType,
        _In_ const std::vector<std::string>& objectIds,
        _In_ const std::vector<std::vector<swss::FieldValueTuple>>& strAttributes)
{
    SWSS_LOG_ENTER();

    if (!m_enableSyncMode)
    {
        return;
    }

    // When in synchronous mode, we need to modify redis database when status
    // is success, since consumer table on synchronous mode is not making redis
    // changes and we only want to apply changes when api succeeded. This
    // applies to init view mode and apply view mode.

    static PerformanceIntervalTimer timer("Syncd::syncUpdateRedisBulkQuadEvent");

    timer.start();

    const std::string strObjectType = sai_serialize_object_type(objectType);

    std::unordered_map<std::string, std::vector<swss::FieldValueTuple>> multiHash;

    std::vector<std::string> keys;

    for (size_t idx = 0; idx < statuses.size(); idx++)
    {
        sai_status_t status = statuses[idx];

        if (status != SAI_STATUS_SUCCESS)
        {
            // in case of failure, don't modify database
            continue;
        }

        auto key = strObjectType + ":" + objectIds.at(idx);

        keys.push_back(key);

        if (api == SAI_COMMON_API_BULK_SET)
        {
            // in case of bulk set operation, it can happen that multiple
            // attributes will be set for the same key, then when we want to
            // push them to redis database, we need to combine all attributes
            // to a single vector of attributes

            multiHash[key].push_back(strAttributes.at(idx).at(0));
        }
        else
        {
            multiHash[key] = strAttributes.at(idx);
        }
    }

    const bool initView = isInitViewMode();

    switch (api)
    {
        case SAI_COMMON_API_BULK_CREATE:

            {
                if (initView)
                    m_client->createTempAsicObjects(multiHash);
                else
                    m_client->createAsicObjects(multiHash);

                break;
            }

        case SAI_COMMON_API_BULK_REMOVE:

            {
                if (initView)
                    m_client->removeTempAsicObjects(keys);
                else
                    m_client->removeAsicObjects(keys);

                break;
            }

        case SAI_COMMON_API_BULK_SET:

            {
                // SET is the same as create
                if (initView)
                    m_client->createTempAsicObjects(multiHash);
                else
                    m_client->createAsicObjects(multiHash);

                break;
            }

        case SAI_COMMON_API_GET:
            break; // ignore get since get is not modifying db

        default:

            SWSS_LOG_THROW("api %d is not supported", api);
    }

    timer.stop();

    timer.inc(statuses.size());
}

sai_status_t Syncd::processQuadEvent(
        _In_ sai_common_api_t api,
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    const std::string& key = kfvKey(kco);
    const std::string& op = kfvOp(kco);

    const std::string& strObjectId = key.substr(key.find(":") + 1);

    sai_object_meta_key_t metaKey;
    sai_deserialize_object_meta_key(key, metaKey);

    if (!sai_metadata_is_object_type_valid(metaKey.objecttype))
    {
        SWSS_LOG_THROW("invalid object type %s", key.c_str());
    }

    auto& values = kfvFieldsValues(kco);

    for (auto& v: values)
    {
        SWSS_LOG_DEBUG("attr: %s: %s", fvField(v).c_str(), fvValue(v).c_str());
    }

    SaiAttributeList list(metaKey.objecttype, values, false);

    /*
     * Attribute list can't be const since we will use it to translate VID to
     * RID in place.
     */

    sai_attribute_t *attr_list = list.get_attr_list();
    uint32_t attr_count = list.get_attr_count();

    /*
     * NOTE: This check pointers must be executed before init view mode, since
     * this methods replaces pointers from orchagent memory space to syncd
     * memory space.
     */

    if (metaKey.objecttype == SAI_OBJECT_TYPE_SWITCH && (api == SAI_COMMON_API_CREATE || api == SAI_COMMON_API_SET))
    {
        /*
         * We don't need to clear those pointers on switch remove (even last),
         * since those pointers will reside inside attributes, also sairedis
         * will internally check whether pointer is null or not, so we here
         * will receive all notifications, but redis only those that were set.
         *
         * TODO: must be done per switch, and switch may not exists yet
         */

        m_handler->updateNotificationsPointers(attr_count, attr_list);
    }

    if (isInitViewMode())
    {
        sai_status_t status = processQuadEventInInitViewMode(metaKey.objecttype, strObjectId, api, attr_count, attr_list);

        syncUpdateRedisQuadEvent(status, api, kco);

        return status;
    }

    if (api != SAI_COMMON_API_GET)
    {
        /*
         * NOTE: we can also call translate on get, if sairedis will clean
         * buffer so then all OIDs will be NULL, and translation will also
         * convert them to NULL.
         */

        SWSS_LOG_DEBUG("translating VID to RIDs on all attributes");

        m_translator->translateVidToRid(metaKey.objecttype, attr_count, attr_list);
    }

    auto info = sai_metadata_get_object_type_info(metaKey.objecttype);

    sai_status_t status;

    if (info->isnonobjectid)
    {
        if (info->objecttype == SAI_OBJECT_TYPE_ROUTE_ENTRY)
        {
            static PerformanceIntervalTimer timer("Syncd::processQuadEvent::processEntry(route_entry)");

            timer.start();

            status = processEntry(metaKey, api, attr_count, attr_list);

            timer.stop();

            timer.inc();
        }
        else
        {
            status = processEntry(metaKey, api, attr_count, attr_list);
        }
    }
    else
    {
        status = processOid(metaKey.objecttype, strObjectId, api, attr_count, attr_list);
    }

    if (api == SAI_COMMON_API_GET)
    {
        if (status != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_INFO("get API for key: %s op: %s returned status: %s",
                    key.c_str(),
                    op.c_str(),
                    sai_serialize_status(status).c_str());
        }

        // extract switch VID from any object type

        sai_object_id_t switchVid = VidManager::switchIdQuery(metaKey.objectkey.key.object_id);

        sendGetResponse(metaKey.objecttype, strObjectId, switchVid, status, attr_count, attr_list);
    }
    else if (status != SAI_STATUS_SUCCESS)
    {
        sendApiResponse(api, status);

        if (info->isobjectid && api == SAI_COMMON_API_SET)
        {
            sai_object_id_t vid = metaKey.objectkey.key.object_id;
            sai_object_id_t rid = m_translator->translateVidToRid(vid);

            SWSS_LOG_ERROR("VID: %s RID: %s",
                    sai_serialize_object_id(vid).c_str(),
                    sai_serialize_object_id(rid).c_str());
        }

        for (const auto &v: values)
        {
            SWSS_LOG_ERROR("attr: %s: %s", fvField(v).c_str(), fvValue(v).c_str());
        }

        if (!m_enableSyncMode)
        {
            // throw only when sync mode is not enabled

            SWSS_LOG_THROW("failed to execute api: %s, key: %s, status: %s",
                    op.c_str(),
                    key.c_str(),
                    sai_serialize_status(status).c_str());
        }
    }
    else // non GET api, status is SUCCESS
    {
        sendApiResponse(api, status);
    }

    syncUpdateRedisQuadEvent(status, api, kco);

    return status;
}

sai_status_t Syncd::processOid(
        _In_ sai_object_type_t objectType,
        _In_ const std::string &strObjectId,
        _In_ sai_common_api_t api,
        _In_ uint32_t attr_count,
        _In_ sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    sai_object_id_t object_id;
    sai_deserialize_object_id(strObjectId, object_id);

    SWSS_LOG_DEBUG("calling %s for %s",
            sai_serialize_common_api(api).c_str(),
            sai_serialize_object_type(objectType).c_str());

    /*
     * We need to do translate vid/rid except for create, since create will
     * create new RID value, and we will have to map them to VID we received in
     * create query.
     */

    auto info = sai_metadata_get_object_type_info(objectType);

    if (info->isnonobjectid)
    {
        SWSS_LOG_THROW("passing non object id %s as generic object", info->objecttypename);
    }

    switch (api)
    {
        case SAI_COMMON_API_CREATE:
            return processOidCreate(objectType, strObjectId, attr_count, attr_list);

        case SAI_COMMON_API_REMOVE:
            return processOidRemove(objectType, strObjectId);

        case SAI_COMMON_API_SET:
            return processOidSet(objectType, strObjectId, attr_list);

        case SAI_COMMON_API_GET:
            return processOidGet(objectType, strObjectId, attr_count, attr_list);

        default:

            SWSS_LOG_THROW("common api (%s) is not implemented", sai_serialize_common_api(api).c_str());
    }
}

sai_status_t Syncd::processOidCreate(
        _In_ sai_object_type_t objectType,
        _In_ const std::string &strObjectId,
        _In_ uint32_t attr_count,
        _In_ sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    sai_object_id_t objectVid;
    sai_deserialize_object_id(strObjectId, objectVid);

    // Object id is VID, we can use it to extract switch id.

    sai_object_id_t switchVid = VidManager::switchIdQuery(objectVid);

    sai_object_id_t switchRid = SAI_NULL_OBJECT_ID;

    if (objectType == SAI_OBJECT_TYPE_SWITCH)
    {
        SWSS_LOG_NOTICE("creating switch number %zu", m_switches.size() + 1);
    }
    else
    {
        /*
         * When we are creating switch, then switchId parameter is ignored, but
         * we can't convert it using vid to rid map, since rid doesn't exist
         * yet, so skip translate for switch, but use translate for all other
         * objects.
         */

        switchRid = m_translator->translateVidToRid(switchVid);
    }

    sai_object_id_t objectRid;

    sai_status_t status = m_vendorSai->create(objectType, &objectRid, switchRid, attr_count, attr_list);

    if (status == SAI_STATUS_SUCCESS)
    {
        /*
         * Object was created so new object id was generated we need to save
         * virtual id's to redis db.
         */

        m_translator->insertRidAndVid(objectRid, objectVid);

        SWSS_LOG_INFO("saved VID %s to RID %s",
                sai_serialize_object_id(objectVid).c_str(),
                sai_serialize_object_id(objectRid).c_str());

        if (objectType == SAI_OBJECT_TYPE_SWITCH)
        {
            /*
             * All needed data to populate switch should be obtained inside SaiSwitch
             * constructor, like getting all queues, ports, etc.
             */

            m_switches[switchVid] = std::make_shared<SaiSwitch>(switchVid, objectRid, m_client, m_translator, m_vendorSai);

            startDiagShell(objectRid);
        }

        if (objectType == SAI_OBJECT_TYPE_PORT)
        {
            m_switches.at(switchVid)->onPostPortCreate(objectRid, objectVid);
        }
    }

    return status;
}

sai_status_t Syncd::processOidRemove(
        _In_ sai_object_type_t objectType,
        _In_ const std::string &strObjectId)
{
    SWSS_LOG_ENTER();

    sai_object_id_t objectVid;
    sai_deserialize_object_id(strObjectId, objectVid);

    sai_object_id_t rid = m_translator->translateVidToRid(objectVid);

    if (objectType == SAI_OBJECT_TYPE_PORT)
    {
        sai_object_id_t switchVid = VidManager::switchIdQuery(objectVid);

        m_switches.at(switchVid)->collectPortRelatedObjects(rid);
    }

    sai_status_t status = m_vendorSai->remove(objectType, rid);

    if (status == SAI_STATUS_SUCCESS)
    {
        // remove all related objects from REDIS DB and also from existing
        // object references since at this point they are no longer valid

        m_translator->eraseRidAndVid(rid, objectVid);

        if (objectType == SAI_OBJECT_TYPE_SWITCH)
        {
            /*
             * On remove switch there should be extra action all local objects
             * and redis object should be removed on remove switch local and
             * redis db objects should be cleared.
             *
             * Currently we don't want to remove switch so we don't need this
             * method, but lets put this as a safety check.
             */

            SWSS_LOG_THROW("remove switch is not implemented, FIXME");
        }
        else
        {
            /*
             * Removing some object succeeded. Let's check if that
             * object was default created object, eg. vlan member.
             * Then we need to update default created object map in
             * SaiSwitch to be in sync, and be prepared for apply
             * view to transfer those synced default created
             * objects to temporary view when it will be created,
             * since that will be out basic switch state.
             *
             * TODO: there can be some issues with reference count
             * like for schedulers on scheduler groups since they
             * should have internal references, and we still need
             * to create dependency tree from saiDiscovery and
             * update those references to track them, this is
             * printed in metadata sanitycheck as "default value
             * needs to be stored".
             *
             * TODO lets add SAI metadata flag for that this will
             * also needs to be of internal/vendor default but we
             * can already deduce that.
             */

            sai_object_id_t switchVid = VidManager::switchIdQuery(objectVid);

            if (m_switches.at(switchVid)->isDiscoveredRid(rid))
            {
                m_switches.at(switchVid)->removeExistingObjectReference(rid);
            }

            if (objectType == SAI_OBJECT_TYPE_PORT)
            {
                m_switches.at(switchVid)->postPortRemove(rid);
            }
        }
    }

    return status;
}

sai_status_t Syncd::processOidSet(
        _In_ sai_object_type_t objectType,
        _In_ const std::string &strObjectId,
        _In_ sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    sai_object_id_t objectVid;
    sai_deserialize_object_id(strObjectId, objectVid);

    sai_object_id_t rid = m_translator->translateVidToRid(objectVid);

    sai_status_t status = m_vendorSai->set(objectType, rid, attr);

    if (Workaround::isSetAttributeWorkaround(objectType, attr->id, status))
    {
        return SAI_STATUS_SUCCESS;
    }

    return status;
}

sai_status_t Syncd::processOidGet(
        _In_ sai_object_type_t objectType,
        _In_ const std::string &strObjectId,
        _In_ uint32_t attr_count,
        _In_ sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    sai_object_id_t objectVid;
    sai_deserialize_object_id(strObjectId, objectVid);

    sai_object_id_t rid = m_translator->translateVidToRid(objectVid);

    return m_vendorSai->get(objectType, rid, attr_count, attr_list);
}

const char* Syncd::profileGetValue(
        _In_ sai_switch_profile_id_t profile_id,
        _In_ const char* variable)
{
    SWSS_LOG_ENTER();

    if (variable == NULL)
    {
        SWSS_LOG_WARN("variable is null");
        return NULL;
    }

    auto it = m_profileMap.find(variable);

    if (it == m_profileMap.end())
    {
        SWSS_LOG_NOTICE("%s: NULL", variable);
        return NULL;
    }

    SWSS_LOG_NOTICE("%s: %s", variable, it->second.c_str());

    return it->second.c_str();
}

int Syncd::profileGetNextValue(
        _In_ sai_switch_profile_id_t profile_id,
        _Out_ const char** variable,
        _Out_ const char** value)
{
    SWSS_LOG_ENTER();

    if (value == NULL)
    {
        SWSS_LOG_INFO("resetting profile map iterator");

        m_profileIter = m_profileMap.begin();
        return 0;
    }

    if (variable == NULL)
    {
        SWSS_LOG_WARN("variable is null");
        return -1;
    }

    if (m_profileIter == m_profileMap.end())
    {
        SWSS_LOG_INFO("iterator reached end");
        return -1;
    }

    *variable = m_profileIter->first.c_str();
    *value = m_profileIter->second.c_str();

    SWSS_LOG_INFO("key: %s:%s", *variable, *value);

    m_profileIter++;

    return 0;
}

void Syncd::loadProfileMap()
{
    SWSS_LOG_ENTER();

    // in case of virtual switch, populate context config
    m_profileMap[SAI_KEY_VS_GLOBAL_CONTEXT] = std::to_string(m_commandLineOptions->m_globalContext);
    m_profileMap[SAI_KEY_VS_CONTEXT_CONFIG] = m_commandLineOptions->m_contextConfig;

    if (m_commandLineOptions->m_profileMapFile.size() == 0)
    {
        SWSS_LOG_NOTICE("profile map file not specified");
        return;
    }

    std::ifstream profile(m_commandLineOptions->m_profileMapFile);

    if (!profile.is_open())
    {
        SWSS_LOG_ERROR("failed to open profile map file: %s: %s",
                m_commandLineOptions->m_profileMapFile.c_str(),
                strerror(errno));

        exit(EXIT_FAILURE);
    }

    // Provide default value at boot up time and let sai profile value
    // Override following values if existing.
    // SAI reads these values at start up time. It would be too late to
    // set these values later when WARM BOOT is detected.

    m_profileMap[SAI_KEY_WARM_BOOT_WRITE_FILE] = DEF_SAI_WARM_BOOT_DATA_FILE;
    m_profileMap[SAI_KEY_WARM_BOOT_READ_FILE]  = DEF_SAI_WARM_BOOT_DATA_FILE;

    std::string line;

    while (getline(profile, line))
    {
        if (line.size() > 0 && (line[0] == '#' || line[0] == ';'))
        {
            continue;
        }

        size_t pos = line.find("=");

        if (pos == std::string::npos)
        {
            SWSS_LOG_WARN("not found '=' in line %s", line.c_str());
            continue;
        }

        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);

        m_profileMap[key] = value;

        SWSS_LOG_INFO("insert: %s:%s", key.c_str(), value.c_str());
    }
}

void Syncd::sendGetResponse(
        _In_ sai_object_type_t objectType,
        _In_ const std::string& strObjectId,
        _In_ sai_object_id_t switchVid,
        _In_ sai_status_t status,
        _In_ uint32_t attr_count,
        _In_ sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    std::vector<swss::FieldValueTuple> entry;

    if (status == SAI_STATUS_SUCCESS)
    {
        m_translator->translateRidToVid(objectType, switchVid, attr_count, attr_list);

        /*
         * Normal serialization + translate RID to VID.
         */

        entry = SaiAttributeList::serialize_attr_list(
                objectType,
                attr_count,
                attr_list,
                false);

        /*
         * All oid values here are VIDs.
         */

        snoopGetResponse(objectType, strObjectId, attr_count, attr_list);
    }
    else if (status == SAI_STATUS_BUFFER_OVERFLOW)
    {
        /*
         * In this case we got correct values for list, but list was too small
         * so serialize only count without list itself, sairedis will need to
         * take this into account when deserialize.
         *
         * If there was a list somewhere, count will be changed to actual value
         * different attributes can have different lists, many of them may
         * serialize only count, and will need to support that on the receiver.
         */

        entry = SaiAttributeList::serialize_attr_list(
                objectType,
                attr_count,
                attr_list,
                true);
    }
    else
    {
        /*
         * Some other error, don't send attributes at all.
         */
    }

    for (const auto &e: entry)
    {
        SWSS_LOG_DEBUG("attr: %s: %s", fvField(e).c_str(), fvValue(e).c_str());
    }

    std::string strStatus = sai_serialize_status(status);

    SWSS_LOG_INFO("sending response for GET api with status: %s", strStatus.c_str());

    /*
     * Since we have only one get at a time, we don't have to serialize object
     * type and object id, only get status is required to be returned.  Get
     * response will not put any data to table, only queue is used.
     */

    m_selectableChannel->set(strStatus, entry, REDIS_ASIC_STATE_COMMAND_GETRESPONSE);

    SWSS_LOG_INFO("response for GET api was send");
}

void Syncd::snoopGetResponse(
        _In_ sai_object_type_t object_type,
        _In_ const std::string& strObjectId, // can be non object id
        _In_ uint32_t attr_count,
        _In_ const sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    /*
     * NOTE: this method is operating on VIDs, all RIDs were translated outside
     * this method.
     */

    /*
     * Vlan (including vlan 1) will need to be put into TEMP view this should
     * also be valid for all objects that were queried.
     */

    for (uint32_t idx = 0; idx < attr_count; ++idx)
    {
        const sai_attribute_t &attr = attr_list[idx];

        auto meta = sai_metadata_get_attr_metadata(object_type, attr.id);

        if (meta == NULL)
        {
            SWSS_LOG_THROW("unable to get metadata for object type %d, attribute %d", object_type, attr.id);
        }

        /*
         * We should snoop oid values even if they are readonly we just note in
         * temp view that those objects exist on switch.
         */

        switch (meta->attrvaluetype)
        {
            case SAI_ATTR_VALUE_TYPE_OBJECT_ID:
                snoopGetOid(attr.value.oid);
                break;

            case SAI_ATTR_VALUE_TYPE_OBJECT_LIST:
                snoopGetOidList(attr.value.objlist);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_FIELD_DATA_OBJECT_ID:
                if (attr.value.aclfield.enable)
                    snoopGetOid(attr.value.aclfield.data.oid);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_FIELD_DATA_OBJECT_LIST:
                if (attr.value.aclfield.enable)
                    snoopGetOidList(attr.value.aclfield.data.objlist);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_ACTION_DATA_OBJECT_ID:
                if (attr.value.aclaction.enable)
                    snoopGetOid(attr.value.aclaction.parameter.oid);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_ACTION_DATA_OBJECT_LIST:
                if (attr.value.aclaction.enable)
                    snoopGetOidList(attr.value.aclaction.parameter.objlist);
                break;

            default:

                /*
                 * If in future new attribute with object id will be added this
                 * will make sure that we will need to add handler here.
                 */

                if (meta->isoidattribute)
                {
                    SWSS_LOG_THROW("attribute %s is object id, but not processed, FIXME", meta->attridname);
                }

                break;
        }

        if (SAI_HAS_FLAG_READ_ONLY(meta->flags))
        {
            /*
             * If value is read only, we skip it, since after syncd restart we
             * won't be able to set/create it anyway.
             */

            continue;
        }

        if (meta->objecttype == SAI_OBJECT_TYPE_PORT &&
                meta->attrid == SAI_PORT_ATTR_HW_LANE_LIST)
        {
            /*
             * Skip port lanes for now since we don't create ports.
             */

            SWSS_LOG_INFO("skipping %s for %s", meta->attridname, strObjectId.c_str());
            continue;
        }

        /*
         * Put non readonly, and non oid attribute value to temp view.
         *
         * NOTE: This will also put create-only attributes to view, and after
         * syncd hard reinit we will not be able to do "SET" on that attribute.
         *
         * Similar action can happen when we will do this on asicSet during
         * apply view.
         */

        snoopGetAttrValue(strObjectId, meta, attr);
    }
}

void Syncd::snoopGetAttr(
        _In_ sai_object_type_t objectType,
        _In_ const std::string& strObjectId,
        _In_ const std::string& attrId,
        _In_ const std::string& attrValue)
{
    SWSS_LOG_ENTER();

    std::string mk = sai_serialize_object_type(objectType) + ":" + strObjectId;

    sai_object_meta_key_t metaKey;
    sai_deserialize_object_meta_key(mk, metaKey);

    if (isInitViewMode())
    {
        m_client->setTempAsicObject(metaKey, attrId, attrValue);
    }
    else
    {
        m_client->setAsicObject(metaKey, attrId, attrValue);
    }
}

void Syncd::snoopGetOid(
        _In_ sai_object_id_t vid)
{
    SWSS_LOG_ENTER();

    if (vid == SAI_NULL_OBJECT_ID)
    {
        // if snooped oid is NULL then we don't need take any action
        return;
    }

    /*
     * We need use redis version of object type query here since we are
     * operating on VID value, and syncd is compiled against real SAI
     * implementation which has different function m_vendorSai->objectTypeQuery.
     */

    sai_object_type_t objectType = VidManager::objectTypeQuery(vid);

    std::string strVid = sai_serialize_object_id(vid);

    snoopGetAttr(objectType, strVid, "NULL", "NULL");
}

void Syncd::snoopGetOidList(
        _In_ const sai_object_list_t& list)
{
    SWSS_LOG_ENTER();

    for (uint32_t i = 0; i < list.count; i++)
    {
        snoopGetOid(list.list[i]);
    }
}

void Syncd::snoopGetAttrValue(
        _In_ const std::string& strObjectId,
        _In_ const sai_attr_metadata_t *meta,
        _In_ const sai_attribute_t& attr)
{
    SWSS_LOG_ENTER();

    std::string value = sai_serialize_attr_value(*meta, attr);

    SWSS_LOG_DEBUG("%s:%s", meta->attridname, value.c_str());

    snoopGetAttr(meta->objecttype, strObjectId, meta->attridname, value);
}

void Syncd::inspectAsic()
{
    SWSS_LOG_ENTER();

    // Fetch all the keys from ASIC DB
    // Loop through all the keys in ASIC DB

    for (const auto &key: m_client->getAsicStateKeys())
    {
        // ASIC_STATE:objecttype:objectid (object id may contain ':')

        auto start = key.find_first_of(":");

        if (start == std::string::npos)
        {
            SWSS_LOG_ERROR("invalid ASIC_STATE_TABLE %s: no start :", key.c_str());
            break;
        }

        auto mk = key.substr(start + 1);

        sai_object_meta_key_t metaKey;
        sai_deserialize_object_meta_key(mk, metaKey);

        // Find all the attrid from ASIC DB, and use them to query ASIC

        auto hash = m_client->getAttributesFromAsicKey(key);

        std::vector<swss::FieldValueTuple> values;

        for (auto &kv: hash)
        {
            const std::string &skey = kv.first;
            const std::string &svalue = kv.second;

            swss::FieldValueTuple fvt(skey, svalue);

            values.push_back(fvt);
        }

        SaiAttributeList list(metaKey.objecttype, values, false);

        sai_attribute_t *attr_list = list.get_attr_list();

        uint32_t attr_count = list.get_attr_count();

        SWSS_LOG_DEBUG("attr count: %u", list.get_attr_count());

        if (attr_count == 0)
        {
            // TODO: how to check ASIC on ASIC DB key with NULL:NULL hash
            // just ignore for now
            continue;
        }

        m_translator->translateVidToRid(metaKey);

        sai_status_t status = m_vendorSai->get(metaKey, attr_count, attr_list);

        if (status != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_ERROR("failed to execute get api on %s: %s",
                    sai_serialize_object_meta_key(metaKey).c_str(),
                    sai_serialize_status(status).c_str());
            continue;
        }

        // compare fields and values from ASIC_DB and SAI response and log the difference

        for (uint32_t index = 0; index < attr_count; ++index)
        {
            const sai_attribute_t& attr = attr_list[index];

            auto meta = sai_metadata_get_attr_metadata(metaKey.objecttype, attr.id);

            if (meta == NULL)
            {
                SWSS_LOG_ERROR("FATAL: failed to find metadata for object type %s and attr id %d",
                        sai_serialize_object_type(metaKey.objecttype).c_str(),
                        attr.id);
                break;
            }

            std::string strSaiAttrValue = sai_serialize_attr_value(*meta, attr, false);

            std::string strRedisAttrValue = hash[meta->attridname];

            if (strRedisAttrValue == strSaiAttrValue)
            {
                SWSS_LOG_INFO("matched %s REDIS and ASIC attr value '%s' with on %s",
                        meta->attridname,
                        strRedisAttrValue.c_str(),
                        sai_serialize_object_meta_key(metaKey).c_str());
            }
            else
            {
                SWSS_LOG_ERROR("failed to match %s REDIS attr '%s' with ASIC attr '%s' for %s",
                        meta->attridname,
                        strRedisAttrValue.c_str(),
                        strSaiAttrValue.c_str(),
                        sai_serialize_object_meta_key(metaKey).c_str());
            }
        }
    }
}

sai_status_t Syncd::processNotifySyncd(
        _In_ const swss::KeyOpFieldsValuesTuple &kco)
{
    SWSS_LOG_ENTER();

    auto& key = kfvKey(kco);

    if (!m_commandLineOptions->m_enableTempView)
    {
        SWSS_LOG_NOTICE("received %s, ignored since TEMP VIEW is not used, returning success", key.c_str());

        sendNotifyResponse(SAI_STATUS_SUCCESS);

        return SAI_STATUS_SUCCESS;
    }

    auto redisNotifySyncd = sai_deserialize_redis_notify_syncd(key);

    if (m_veryFirstRun && m_firstInitWasPerformed && redisNotifySyncd == SAI_REDIS_NOTIFY_SYNCD_INIT_VIEW)
    {
        /*
         * Make sure that when second INIT view arrives, then we will jump to
         * next section, since second init view may create switch that already
         * exists and will fail with creating multiple switches error.
         */

        m_veryFirstRun = false;
    }
    else if (m_veryFirstRun)
    {
        SWSS_LOG_NOTICE("very first run is TRUE, op = %s", key.c_str());

        sai_status_t status = SAI_STATUS_SUCCESS;

        /*
         * On the very first start of syncd, "compile" view is directly applied
         * on device, since it will make it easier to switch to new asic state
         * later on when we restart orch agent.
         */

        if (redisNotifySyncd == SAI_REDIS_NOTIFY_SYNCD_INIT_VIEW)
        {
            /*
             * On first start we just do "apply" directly on asic so we set
             * init to false instead of true.
             */

            m_asicInitViewMode = false;

            m_firstInitWasPerformed = true;

            // we need to clear current temp view to make space for new one

            clearTempView();
        }
        else if (redisNotifySyncd == SAI_REDIS_NOTIFY_SYNCD_APPLY_VIEW)
        {
            m_veryFirstRun = false;

            m_asicInitViewMode = false;

            if (m_commandLineOptions->m_startType == SAI_START_TYPE_FASTFAST_BOOT)
            {
                // fastfast boot configuration end

                status = onApplyViewInFastFastBoot();
            }

            SWSS_LOG_NOTICE("setting very first run to FALSE, op = %s", key.c_str());
        }
        else
        {
            SWSS_LOG_THROW("unknown operation: %s", key.c_str());
        }

        sendNotifyResponse(status);

        return status;
    }

    if (redisNotifySyncd == SAI_REDIS_NOTIFY_SYNCD_INIT_VIEW)
    {
        if (m_asicInitViewMode)
        {
            SWSS_LOG_WARN("syncd is already in asic INIT VIEW mode, but received init again, orchagent restarted before apply?");
        }

        m_asicInitViewMode = true;

        clearTempView();

        // NOTE: Currently as WARN to be easier to spot, later should be NOTICE.

        SWSS_LOG_WARN("syncd switched to INIT VIEW mode, all op will be saved to TEMP view");

        sendNotifyResponse(SAI_STATUS_SUCCESS);
    }
    else if (redisNotifySyncd == SAI_REDIS_NOTIFY_SYNCD_APPLY_VIEW)
    {
        m_asicInitViewMode = false;

        // NOTE: Currently as WARN to be easier to spot, later should be NOTICE.

        SWSS_LOG_WARN("syncd received APPLY VIEW, will translate");

        sai_status_t status;

        try
        {
            status = applyView();
        }
        catch(...)
        {
            /*
             * If apply view will fail with exception, try to send fail
             * response to sairedis, since later there can be switch shutdown
             * notification sent, and it will be synchronized with mutex, and
             * it will not be processed until get response timeout will hit.
             */

            sendNotifyResponse(SAI_STATUS_FAILURE);

            throw;
        }

        sendNotifyResponse(status);

        if (status == SAI_STATUS_SUCCESS)
        {
            /*
             * We successfully applied new view, VID mapping could change, so
             * we need to clear local db, and all new VIDs will be queried
             * using redis.
             *
             * TODO possible race condition - get notification when new view is
             * applied and cache have old values, and notification start's
             * translating vid/rid, we need to stop processing notifications
             * for transition (queue can still grow), possible fdb
             * notifications but fdb learning was disabled on warm boot, so
             * there should be no issue.
             */

            m_translator->clearLocalCache();
        }
        else
        {
            /*
             * Apply view failed. It can fail in 2 ways, ether nothing was
             * executed, on asic, or asic is inconsistent state then we should
             * die or hang.
             */

            return status;
        }
    }
    else if (redisNotifySyncd == SAI_REDIS_NOTIFY_SYNCD_INSPECT_ASIC)
    {
        SWSS_LOG_NOTICE("syncd switched to INSPECT ASIC mode");

        inspectAsic();

        sendNotifyResponse(SAI_STATUS_SUCCESS);
    }
    else
    {
        SWSS_LOG_ERROR("unknown operation: %s", key.c_str());

        sendNotifyResponse(SAI_STATUS_NOT_IMPLEMENTED);

        SWSS_LOG_THROW("notify syncd %s operation failed", key.c_str());
    }

    return SAI_STATUS_SUCCESS;
}

void Syncd::sendNotifyResponse(
        _In_ sai_status_t status)
{
    SWSS_LOG_ENTER();

    std::string strStatus = sai_serialize_status(status);

    std::vector<swss::FieldValueTuple> entry;

    SWSS_LOG_INFO("sending response: %s", strStatus.c_str());

    m_selectableChannel->set(strStatus, entry, REDIS_ASIC_STATE_COMMAND_NOTIFY);
}

void Syncd::clearTempView()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("clearing current TEMP VIEW");

    SWSS_LOG_TIMER("clear temp view");

    m_client->removeTempAsicStateTable();

    // Also clear list of objects removed in init view mode.

    m_initViewRemovedVidSet.clear();
}

sai_status_t Syncd::onApplyViewInFastFastBoot()
{
    SWSS_LOG_ENTER();

    sai_status_t all = SAI_STATUS_SUCCESS;

    for (auto& kvp: m_switches)
    {
        sai_attribute_t attr;

        attr.id = SAI_SWITCH_ATTR_FAST_API_ENABLE;
        attr.value.booldata = false;

        sai_status_t status = m_vendorSai->set(SAI_OBJECT_TYPE_SWITCH, kvp.second->getRid(), &attr);

        if (status != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_ERROR("Failed to set SAI_SWITCH_ATTR_FAST_API_ENABLE=false: %s for switch RID: %s",
                    sai_serialize_status(status).c_str(),
                    sai_serialize_object_id(kvp.second->getRid()).c_str());

            all = status;
        }
    }

    return all;
}

sai_status_t Syncd::applyView()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_TIMER("apply");

    /*
     * We assume that there will be no case that we will move from 1 to 0, also
     * if at the beginning there is no switch, then when user will send create,
     * and it will be actually created (real call) so there should be no case
     * when we are moving from 0 -> 1.
     */

    /*
     * This method contains 2 stages.
     *
     * First stage is non destructive, when orchagent will build new view, and
     * there will be bug in comparison logic in first stage, then syncd will
     * send failure when doing apply view to orchagent but it will still be
     * running. No asic operations are performed during this stage.
     *
     * Second stage is destructive, so if there will be bug in comparison logic
     * or any asic operation will fail, then syncd will crash, since asic will
     * be in inconsistent state.
     */

    /*
     * Initialize rand for future candidate object selection if necessary.
     *
     * NOTE: Should this be deterministic? So we could repeat random choice
     * when something bad happen or we hit a bug, so in that case it will be
     * easier for reproduce, we could at least log value returned from time().
     *
     * TODO: To make it stable, we also need to make stable redisGetAsicView
     * since now order of items is random. Also redis result needs to be
     * sorted.
     */

    // Read current and temporary views from REDIS.

    auto currentMap = m_client->getAsicView();
    auto temporaryMap = m_client->getTempAsicView();

    if (currentMap.size() != temporaryMap.size())
    {
        SWSS_LOG_THROW("current view switches: %zu != temporary view switches: %zu, FATAL",
                currentMap.size(),
                temporaryMap.size());
    }

    if (currentMap.size() != m_switches.size())
    {
        SWSS_LOG_THROW("current asic view switches %zu != defined switches %zu, FATAL",
                currentMap.size(),
                m_switches.size());
    }

    // VID of switches must match for each map

    for (auto& kvp: currentMap)
    {
        if (temporaryMap.find(kvp.first) == temporaryMap.end())
        {
            SWSS_LOG_THROW("switch VID %s missing from temporary view!, FATAL",
                    sai_serialize_object_id(kvp.first).c_str());
        }

        if (m_switches.find(kvp.first) == m_switches.end())
        {
            SWSS_LOG_THROW("switch VID %s missing from ASIC, FATAL",
                    sai_serialize_object_id(kvp.first).c_str());
        }
    }

    std::vector<std::shared_ptr<AsicView>> currentViews;
    std::vector<std::shared_ptr<AsicView>> tempViews;
    std::vector<std::shared_ptr<ComparisonLogic>> cls;

    try
    {
        for (auto& kvp: m_switches)
        {
            auto switchVid = kvp.first;

            auto sw = m_switches.at(switchVid);

            /*
             * We are starting first stage here, it still can throw exceptions
             * but it's non destructive for ASIC, so just catch and return in
             * case of failure.
             *
             * Each ASIC view at this point will contain only 1 switch.
             */

            auto current = std::make_shared<AsicView>(currentMap.at(switchVid));
            auto temp = std::make_shared<AsicView>(temporaryMap.at(switchVid));

            auto cl = std::make_shared<ComparisonLogic>(m_vendorSai, sw, m_handler, m_initViewRemovedVidSet, current, temp, m_breakConfig);

            cl->compareViews();

            currentViews.push_back(current);
            tempViews.push_back(temp);
            cls.push_back(cl);
        }
    }
    catch (const std::exception &e)
    {
        /*
         * Exception was thrown in first stage, those were non destructive
         * actions so just log exception and let syncd running.
         */

        SWSS_LOG_ERROR("Exception: %s", e.what());

        return SAI_STATUS_FAILURE;
    }

    /*
     * This is second stage. Those operations are destructive, if any of them
     * fail, then we will have inconsistent state in ASIC.
     */

    if (m_commandLineOptions->m_enableUnittests)
    {
        dumpComparisonLogicOutput(currentViews);
    }

    for (auto& cl: cls)
    {
        cl->executeOperationsOnAsic(); // can throw, if so asic will be in inconsistent state
    }

    updateRedisDatabase(tempViews);

    for (auto& cl: cls)
    {
        if (m_commandLineOptions->m_enableConsistencyCheck)
        {
            bool consistent = cl->checkAsicVsDatabaseConsistency(m_translator);

            if (!consistent && m_commandLineOptions->m_enableUnittests)
            {
                SWSS_LOG_THROW("ASIC content is different than DB content!");
            }
        }
    }

    return SAI_STATUS_SUCCESS;
}

void Syncd::dumpComparisonLogicOutput(
    _In_ const std::vector<std::shared_ptr<AsicView>>& currentViews)
{
    SWSS_LOG_ENTER();

    std::stringstream ss;

    size_t total = 0; // total operations from all switches

    for (auto& c: currentViews)
    {
        total += c->asicGetOperationsCount();
    }

    ss << "ASIC_OPERATIONS: " << total << std::endl;

    for (auto& c: currentViews)
    {
        ss << "ASIC_OPERATIONS on "
            << sai_serialize_object_id(c->getSwitchVid())
            << " : "
            << c->asicGetOperationsCount()
            << std::endl;

        for (const auto &op: c->asicGetWithOptimizedRemoveOperations())
        {
            const std::string &key = kfvKey(*op.m_op);
            const std::string &opp = kfvOp(*op.m_op);

            ss << "o " << opp << ": " << key << std::endl;

            const auto &values = kfvFieldsValues(*op.m_op);

            for (auto v: values)
                ss << "a: " << fvField(v) << " " << fvValue(v) << std::endl;
        }
    }

    std::ofstream log("applyview.log");

    if (log.is_open())
    {
        log << ss.str();

        log.close();

        SWSS_LOG_NOTICE("wrote apply_view asic operations to applyview.log");
    }
    else
    {
        SWSS_LOG_ERROR("failed to open applyview.log");
    }
}

void Syncd::updateRedisDatabase(
    _In_ const std::vector<std::shared_ptr<AsicView>>& temporaryViews)
{
    SWSS_LOG_ENTER();

    // TODO: We can make LUA script for this which will be much faster.
    //
    // TODO: Needs to be revisited if ASIC views will be across multiple redis
    // database indexes.

    SWSS_LOG_TIMER("redis update");

    m_client->removeAsicStateTable();

    m_client->removeTempAsicStateTable();

    // Save temporary views as current view in redis database.

    for (auto& tv: temporaryViews)
    {
        for (const auto &pair: tv->m_soAll)
        {
            const auto &obj = pair.second;

            const auto &attr = obj->getAllAttributes();

            std::vector<swss::FieldValueTuple> entry;

            for (const auto &ap: attr)
            {
                const auto saiAttr = ap.second;

                entry.emplace_back(saiAttr->getStrAttrId(), saiAttr->getStrAttrValue());
            }

            m_client->createAsicObject(obj->m_meta_key, entry);
        }
    }

    /*
     * Remove previous RID2VID maps and apply new map.
     *
     * NOTE: This needs to be done per switch, we can't remove all maps.
     */

    // TODO check if those 2 maps are consistent

    std::unordered_map<sai_object_id_t, sai_object_id_t> allVid2Rid;

    for (auto& tv: temporaryViews)
    {
        for (auto &kv: tv->m_ridToVid)
        {
            allVid2Rid[kv.second] = kv.first;
        }
    }

    m_client->setVidAndRidMap(allVid2Rid);

    SWSS_LOG_NOTICE("updated redis database");
}

// TODO for future we can have each switch in separate redis db index or even
// some switches in the same db index and some in separate.  Current redis get
// asic view is assuming all switches are in the same db index an also some
// operations per switch are accessing data base in SaiSwitch class.  This
// needs to be reorganised to access database per switch basis and get only
// data that corresponds to each particular switch and access correct db index.

void Syncd::onSyncdStart(
        _In_ bool warmStart)
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    /*
     * It may happen that after initialize we will receive some port
     * notifications with port'ids that are not in redis db yet, so after
     * checking VIDTORID map there will be entries and translate_vid_to_rid
     * will generate new id's for ports, this may cause race condition so we
     * need to use a lock here to prevent that.
     */

    SWSS_LOG_TIMER("on syncd start");

    if (warmStart)
    {
        /*
         * Switch was warm started, so switches map is empty, we need to
         * recreate it based on existing entries inside database.
         *
         * Currently we expect only one switch, then we need to call it.
         *
         * Also this will make sure that current switch id is the same as
         * before restart.
         *
         * If we want to support multiple switches, this needs to be adjusted.
         */

        performWarmRestart();

        SWSS_LOG_NOTICE("skipping hard reinit since WARM start was performed");
        return;
    }

    SWSS_LOG_NOTICE("performing hard reinit since COLD start was performed");

    /*
     * Switch was restarted in hard way, we need to perform hard reinit and
     * recreate switches map.
     */

    if (m_switches.size())
    {
        SWSS_LOG_THROW("performing hard reinit, but there are %zu switches defined, bug!", m_switches.size());
    }

    HardReiniter hr(m_client, m_translator, m_vendorSai, m_handler);

    m_switches = hr.hardReinit();

    for (auto& sw: m_switches)
    {
        startDiagShell(sw.second->getRid());
    }

    SWSS_LOG_NOTICE("hard reinit succeeded");
}

void Syncd::onSwitchCreateInInitViewMode(
        _In_ sai_object_id_t switchVid,
        _In_ uint32_t attr_count,
        _In_ const sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    /*
     * We can have multiple switches here, but each switch is identified by
     * SAI_SWITCH_ATTR_SWITCH_HARDWARE_INFO. This attribute is treated as key,
     * so each switch will have different hardware info.
     *
     * Currently we assume that we have only one switch.
     *
     * We can have 2 scenarios here:
     *
     * - we have multiple switches already existing, and in init view mode user
     *   will create the same switches, then since switch id are deterministic
     *   we can match them by hardware info and by switch id, it may happen
     *   that switch id will be different if user will create switches in
     *   different order, this case will be not supported unless special logic
     *   will be written to handle that case. This case is solved by bounding
     *   hardware info to switch index in context config file.
     *
     * - if user created switches but non of switch has the same hardware info
     *   then it means we need to create actual switch here, since user will
     *   want to query switch ports etc values, that's why on create switch is
     *   special case, and that's why we need to keep track of all switches.
     *   This case is also solved bu allowing creation of only switches defined
     *   in context config which bounds hardware info and switch index making
     *   switch VID deterministic.
     *
     * Since we are creating switch here, we are sure that this switch don't
     * have any oid attributes set, so we can pass all attributes.
     *
     * Hardware info attribute must be passed and all non OID attributes
     * including create only and conditionals.
     */

    /*
     * Multiple switches scenario with changed order:
     *
     * If orchagent will create the same switch with the same hardware info but
     * with different order since switch id is deterministic, then VID of both
     * switches will always match since they are bound to hardware info using
     * context config file.
     */

    if (m_switches.find(switchVid) == m_switches.end())
    {
        /*
         * Switch with particular VID don't exists yet, so lets create it.  We
         * need to create this switch so user in init mode could query switch
         * properties using GET api.
         *
         * We assume that none of attributes is object id attribute.
         *
         * This scenario can happen when you start syncd on empty database and
         * then you quit and restart it again.
         */

        sai_object_id_t switchRid;

        sai_status_t status;

        {
            SWSS_LOG_TIMER("cold boot: create switch");

            status = m_vendorSai->create(SAI_OBJECT_TYPE_SWITCH, &switchRid, 0, attr_count, attr_list);
        }

        if (status != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_THROW("failed to create switch in init view mode: %s",
                    sai_serialize_status(status).c_str());
        }

        /*
         * Object was created so new RID was generated we need to save virtual
         * id's to redis db.
         */

        SWSS_LOG_NOTICE("created switch VID %s to RID %s in init view mode",
                sai_serialize_object_id(switchVid).c_str(),
                sai_serialize_object_id(switchRid).c_str());

        m_translator->insertRidAndVid(switchRid, switchVid);

        // make switch initialization and get all default data

        m_switches[switchVid] = std::make_shared<SaiSwitch>(switchVid, switchRid, m_client, m_translator, m_vendorSai);

        startDiagShell(switchRid);
    }
    else
    {
        /*
         * There is already switch defined, we need to match it by hardware
         * info and we need to know that current switch VID also should match
         * since it's deterministic created.
         */

        auto sw = m_switches.at(switchVid);

        // switches VID must match, since it's deterministic

        if (switchVid != sw->getVid())
        {
            SWSS_LOG_THROW("created switch VID don't match: previous %s, current: %s",
                    sai_serialize_object_id(switchVid).c_str(),
                    sai_serialize_object_id(sw->getVid()).c_str());
        }

        // also hardware info also must match

        std::string currentHw = sw->getHardwareInfo();
        std::string newHw;

        auto attr = sai_metadata_get_attr_by_id(SAI_SWITCH_ATTR_SWITCH_HARDWARE_INFO, attr_count, attr_list);

        if (attr == NULL)
        {
            // this is ok, attribute doesn't exist, so assumption is empty string
        }
        else
        {
            newHw = std::string((char*)attr->value.s8list.list, attr->value.s8list.count);
        }

        SWSS_LOG_NOTICE("new switch %s contains hardware info: '%s'",
                sai_serialize_object_id(switchVid).c_str(),
                newHw.c_str());

        if (currentHw != newHw)
        {
            SWSS_LOG_THROW("hardware info mismatch: current '%s' vs new '%s'", currentHw.c_str(), newHw.c_str());
        }

        SWSS_LOG_NOTICE("current %s switch hardware info: '%s'",
                sai_serialize_object_id(switchVid).c_str(),
                currentHw.c_str());

        /*
         * Some attributes on new switch could be different then on existing
         * one, but we are in init view mode so comparison logic will be
         * executed on apply view and those attributes will be compared and
         * actions will be generated if any of them are different.
         */
    }
}

void Syncd::performWarmRestartSingleSwitch(
        _In_ const std::string& key)
{
    SWSS_LOG_ENTER();

    // key should be in format ASIC_STATE:SAI_OBJECT_TYPE_SWITCH:oid:0xYYYY

    /*
     * Since multiple switches can be defined on warm boot, then we need to
     * correctly identify each switch by passing hardware info.
     *
     * TODO: do we also need to pass any other attributes, like create only etc?
     */

    auto start = key.find_first_of(":") + 1;
    auto end = key.find(":", start);

    std::string strSwitchVid = key.substr(end + 1);

    std::vector<swss::FieldValueTuple> values;

    auto hash = m_client->getAttributesFromAsicKey(key);

    SWSS_LOG_NOTICE("switch %s", strSwitchVid.c_str());

    for (auto &kv: hash)
    {
        const std::string& skey = kv.first;
        const std::string& svalue = kv.second;

        if (skey == "NULL")
            continue;

        SWSS_LOG_NOTICE(" - attr: %s:%s", skey.c_str(), svalue.c_str());

        swss::FieldValueTuple fvt(skey, svalue);

        values.push_back(fvt);
    }

    SaiAttributeList list(SAI_OBJECT_TYPE_SWITCH, values, false);

    sai_object_id_t switchVid;

    sai_deserialize_object_id(strSwitchVid, switchVid);

    sai_object_id_t originalSwitchRid = m_translator->translateVidToRid(switchVid);

    sai_object_id_t switchRid;

    sai_attr_id_t notifs[] = {
        SAI_SWITCH_ATTR_SWITCH_STATE_CHANGE_NOTIFY,
        SAI_SWITCH_ATTR_SHUTDOWN_REQUEST_NOTIFY,
        SAI_SWITCH_ATTR_FDB_EVENT_NOTIFY,
        SAI_SWITCH_ATTR_PORT_STATE_CHANGE_NOTIFY,
        SAI_SWITCH_ATTR_QUEUE_PFC_DEADLOCK_NOTIFY
    };

    std::vector<sai_attribute_t> attrs;

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_INIT_SWITCH;
    attr.value.booldata = true;

    attrs.push_back(attr);

    for (size_t idx = 0; idx < (sizeof(notifs) / sizeof(notifs[0])); idx++)
    {
        attr.id = notifs[idx];
        attr.value.ptr = (void*)1; // any non-null pointer
    }

    sai_attribute_t *attrList = list.get_attr_list();

    uint32_t attrCount = list.get_attr_count();

    for (uint32_t idx = 0; idx < attrCount; idx++)
    {
        auto id = attrList[idx].id;

        if (id == SAI_SWITCH_ATTR_INIT_SWITCH)
            continue;

        auto meta = sai_metadata_get_attr_metadata(SAI_OBJECT_TYPE_SWITCH, id);

        /*
         * If we want to handle multiple switches, then during warm boot switch
         * create we need to pass hardware info so vendor sai could know which
         * switch to initialize. We also need to update pointer values since
         * new process could be loaded at different address space.
         */

        if (id == SAI_SWITCH_ATTR_SWITCH_HARDWARE_INFO || meta->attrvaluetype == SAI_ATTR_VALUE_TYPE_POINTER)
        {
            attrs.push_back(attrList[idx]);
            continue;
        }

        SWSS_LOG_NOTICE("skipping warm boot: %s", meta->attridname);
    }

    // TODO support multiple notification handlers
    m_handler->updateNotificationsPointers((uint32_t)attrs.size(), attrs.data());

    sai_status_t status;

    {
        SWSS_LOG_TIMER("Warm boot: create switch VID: %s", sai_serialize_object_id(switchVid).c_str());

        status = m_vendorSai->create(SAI_OBJECT_TYPE_SWITCH, &switchRid, 0, (uint32_t)attrs.size(), attrs.data());
    }

    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_THROW("failed to create switch RID: %s for VID %s",
                       sai_serialize_status(status).c_str(),
                       sai_serialize_object_id(switchVid).c_str());
    }

    if (originalSwitchRid != switchRid)
    {
        SWSS_LOG_THROW("Unexpected RID 0x%" PRIx64 " (expected 0x%" PRIx64 " )",
                       switchRid, originalSwitchRid);
    }

    // perform all get operations on existing switch

    auto sw = m_switches[switchVid] = std::make_shared<SaiSwitch>(switchVid, switchRid, m_client, m_translator, m_vendorSai, true);

    startDiagShell(switchRid);
}

void Syncd::performWarmRestart()
{
    SWSS_LOG_ENTER();

    /*
     * There should be no case when we are doing warm restart and there is no
     * switch defined, we will throw at such a case.
     *
     * This case could be possible when no switches were created and only api
     * was initialized, but we will skip this scenario and address is when we
     * will have need for it.
     */

    auto entries = m_client->getAsicStateSwitchesKeys();

    if (entries.size() == 0)
    {
        SWSS_LOG_THROW("on warm restart there is no switches defined in DB, not supported yet, FIXME");
    }

    SWSS_LOG_NOTICE("switches defined in warm restart: %zu", entries.size());

    // here we could have multiple switches defined, let's process them one by one

    for (auto& entry: entries)
    {
        performWarmRestartSingleSwitch(entry);
    }
}

void Syncd::startDiagShell(
        _In_ sai_object_id_t switchRid)
{
    SWSS_LOG_ENTER();

    if (m_commandLineOptions->m_enableDiagShell)
    {
        SWSS_LOG_NOTICE("starting diag shell thread for switch RID %s",
                sai_serialize_object_id(switchRid).c_str());

        std::thread thread = std::thread(&Syncd::diagShellThreadProc, this, switchRid);

        thread.detach();
    }
}

void Syncd::diagShellThreadProc(
        _In_ sai_object_id_t switchRid)
{
    SWSS_LOG_ENTER();

    sai_status_t status;

    /*
     * This is currently blocking API on broadcom, it will block until we exit
     * shell.
     */

    while (true)
    {
        sai_attribute_t attr;
        attr.id = SAI_SWITCH_ATTR_SWITCH_SHELL_ENABLE;
        attr.value.booldata = true;

        status = m_vendorSai->set(SAI_OBJECT_TYPE_SWITCH, switchRid, &attr);

        if (status != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_ERROR("Failed to enable switch shell: %s",
                    sai_serialize_status(status).c_str());
            return;
        }

        sleep(1);
    }
}

void Syncd::sendShutdownRequest(
        _In_ sai_object_id_t switchVid)
{
    SWSS_LOG_ENTER();

    if (m_notifications == nullptr)
    {
        SWSS_LOG_WARN("notifications pointer is NULL");
        return;
    }

    auto s = sai_serialize_object_id(switchVid);

    SWSS_LOG_NOTICE("sending switch_shutdown_request notification to OA for switch: %s", s.c_str());

    std::vector<swss::FieldValueTuple> entry;

    // TODO use m_handler->onSwitchShutdownRequest(switchVid); (but this should be per switch)

    s = sai_serialize_switch_shutdown_request(switchVid);

    m_notifications->send(SAI_SWITCH_NOTIFICATION_NAME_SWITCH_SHUTDOWN_REQUEST, s, entry);
}

void Syncd::sendShutdownRequestAfterException()
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    try
    {
        if (m_switches.size())
        {
            for (auto& kvp: m_switches)
            {
                sendShutdownRequest(kvp.second->getVid());
            }
        }
        else
        {
            sendShutdownRequest(SAI_NULL_OBJECT_ID);
        }

        SWSS_LOG_NOTICE("notification send successfully");
    }
    catch(const std::exception &e)
    {
        SWSS_LOG_ERROR("Runtime error: %s", e.what());
    }
    catch(...)
    {
        SWSS_LOG_ERROR("Unknown runtime error");
    }
}

void Syncd::saiLoglevelNotify(
        _In_ std::string strApi,
        _In_ std::string strLogLevel)
{
    SWSS_LOG_ENTER();

    try
    {
        sai_log_level_t logLevel;
        sai_deserialize_log_level(strLogLevel, logLevel);

        sai_api_t api;
        sai_deserialize_api(strApi, api);

        sai_status_t status = m_vendorSai->logSet(api, logLevel);

        if (status == SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_NOTICE("Setting SAI loglevel %s on %s", strLogLevel.c_str(), strApi.c_str());
        }
        else
        {
            SWSS_LOG_INFO("set loglevel failed: %s", sai_serialize_status(status).c_str());
        }
    }
    catch (const std::exception& e)
    {
        SWSS_LOG_ERROR("Failed to set loglevel to %s on %s: %s",
                strLogLevel.c_str(),
                strApi.c_str(),
                e.what());
    }
}

void Syncd::setSaiApiLogLevel()
{
    SWSS_LOG_ENTER();

    // We start from 1 since 0 is SAI_API_UNSPECIFIED.

    for (uint32_t idx = 1; idx < sai_metadata_enum_sai_api_t.valuescount; ++idx)
    {
        // NOTE: link to db is singleton, so if we would want multiple Syncd
        // instances running at the same process, we need to have logger
        // registrar similar to net link messages

        swss::Logger::linkToDb(
                sai_metadata_enum_sai_api_t.valuesnames[idx],
                std::bind(&Syncd::saiLoglevelNotify, this, _1, _2),
                sai_serialize_log_level(SAI_LOG_LEVEL_NOTICE));
    }
}

sai_status_t Syncd::removeAllSwitches()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("Removing all switches");

    // TODO mutex ?

    sai_status_t result = SAI_STATUS_SUCCESS;

    for (auto& sw: m_switches)
    {
        auto rid = sw.second->getRid();

        auto strRid = sai_serialize_object_id(rid);

        SWSS_LOG_TIMER("removing switch RID %s", strRid.c_str());

        auto status = m_vendorSai->remove(SAI_OBJECT_TYPE_SWITCH, rid);

        if (status != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_NOTICE("Can't delete a switch RID %s: %s",
                    strRid.c_str(),
                    sai_serialize_status(status).c_str());

            result = status;
        }
    }

    return result;
}

sai_status_t Syncd::setRestartWarmOnAllSwitches(
        _In_ bool flag)
{
    SWSS_LOG_ENTER();

    sai_status_t result = SAI_STATUS_SUCCESS;

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_RESTART_WARM;
    attr.value.booldata = flag;

    for (auto& sw: m_switches)
    {
        auto rid = sw.second->getRid();

        auto strRid = sai_serialize_object_id(rid);

        auto status = m_vendorSai->set(SAI_OBJECT_TYPE_SWITCH, rid, &attr);

        if (status != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_ERROR("Failed to set SAI_SWITCH_ATTR_RESTART_WARM=%s: %s:%s",
                    (flag ? "true" : "false"),
                    strRid.c_str(),
                    sai_serialize_status(status).c_str());

            result = status;
        }
    }

    return result;
}

sai_status_t Syncd::setPreShutdownOnAllSwitches()
{
    SWSS_LOG_ENTER();

    sai_status_t result = SAI_STATUS_SUCCESS;

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_PRE_SHUTDOWN;
    attr.value.booldata = true;

    for (auto& sw: m_switches)
    {
        auto rid = sw.second->getRid();

        auto strRid = sai_serialize_object_id(rid);

        auto status = m_vendorSai->set(SAI_OBJECT_TYPE_SWITCH, rid, &attr);

        if (status != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_ERROR("Failed to set SAI_SWITCH_ATTR_PRE_SHUTDOWN=true: %s:%s",
                    strRid.c_str(),
                    sai_serialize_status(status).c_str());

            result = status;
        }
    }

    return result;
}

sai_status_t Syncd::setUninitDataPlaneOnRemovalOnAllSwitches()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("Fast/warm reboot requested, keeping data plane running");

    sai_status_t result = SAI_STATUS_SUCCESS;

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_UNINIT_DATA_PLANE_ON_REMOVAL;
    attr.value.booldata = false;

    for (auto& sw: m_switches)
    {
        auto rid = sw.second->getRid();

        auto strRid = sai_serialize_object_id(rid);

        auto status = m_vendorSai->set(SAI_OBJECT_TYPE_SWITCH, rid, &attr);

        if (status != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_ERROR("Failed to set SAI_SWITCH_ATTR_UNINIT_DATA_PLANE_ON_REMOVAL=false: %s:%s",
                    strRid.c_str(),
                    sai_serialize_status(status).c_str());

            result = status;
        }
    }

    return result;
}

void Syncd::syncProcessNotification(
        _In_ const swss::KeyOpFieldsValuesTuple& item)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    SWSS_LOG_ENTER();

    m_processor->syncProcessNotification(item);
}

bool Syncd::isVeryFirstRun()
{
    SWSS_LOG_ENTER();

    /*
     * If lane map is not defined in redis db then we assume this is very first
     * start of syncd later on we can add additional checks here.
     *
     * TODO: if we add more switches then we need lane maps per switch.
     * TODO: we also need other way to check if this is first start
     *
     * We could use VIDCOUNTER also, but if something is defined in the DB then
     * we assume this is not the first start.
     *
     * TODO we need to fix this, since when there will be queue, it will still think
     * this is first run, let's query HIDDEN ?
     */

    bool firstRun = m_client->hasNoHiddenKeysDefined();

    SWSS_LOG_NOTICE("First Run: %s", firstRun ? "True" : "False");

    return firstRun;
}

static void timerWatchdogCallback(
        _In_ int64_t span)
{
    SWSS_LOG_ENTER();

    SWSS_LOG_ERROR("main loop execution exceeded %ld ms", span/1000);
}

void Syncd::run()
{
    SWSS_LOG_ENTER();

    WarmRestartTable warmRestartTable("STATE_DB"); // TODO from config

    syncd_restart_type_t shutdownType = SYNCD_RESTART_TYPE_COLD;

    volatile bool runMainLoop = true;

    std::shared_ptr<swss::Select> s = std::make_shared<swss::Select>();

    try
    {
        onSyncdStart(m_commandLineOptions->m_startType == SAI_START_TYPE_WARM_BOOT);

        // create notifications processing thread after we create_switch to
        // make sure, we have switch_id translated to VID before we start
        // processing possible quick fdb notifications, and pointer for
        // notification queue is created before we create switch
        m_processor->startNotificationsProcessingThread();

        SWSS_LOG_NOTICE("syncd listening for events");

        s->addSelectable(m_selectableChannel.get());
        s->addSelectable(m_restartQuery.get());
        s->addSelectable(m_flexCounter.get());
        s->addSelectable(m_flexCounterGroup.get());

        SWSS_LOG_NOTICE("starting main loop");
    }
    catch(const std::exception &e)
    {
        SWSS_LOG_ERROR("Runtime error during syncd init: %s", e.what());

        sendShutdownRequestAfterException();

        s = std::make_shared<swss::Select>();

        s->addSelectable(m_restartQuery.get());

        SWSS_LOG_NOTICE("starting main loop, ONLY restart query");

        if (m_commandLineOptions->m_disableExitSleep)
            runMainLoop = false;
    }

    TimerWatchdog twd(30 * 1000000); // watch for executions over 30 seconds

    twd.setCallback(timerWatchdogCallback);

    while(runMainLoop)
    {
        try
        {
            swss::Selectable *sel = NULL;

            int result = s->select(&sel);

            twd.setStartTime();

            if (sel == m_restartQuery.get())
            {
                /*
                 * This is actual a bad design, since selectable may pick up
                 * multiple events from the queue, and after restart those
                 * events will be forgotten since they were consumed already and
                 * this may lead to forget populate object table which will
                 * lead to unable to find some objects.
                 */

                SWSS_LOG_NOTICE("is asic queue empty: %d", m_selectableChannel->empty());

                while (!m_selectableChannel->empty())
                {
                    processEvent(*m_selectableChannel.get());
                }

                SWSS_LOG_NOTICE("drained queue");

                shutdownType = handleRestartQuery(*m_restartQuery);

                if (shutdownType != SYNCD_RESTART_TYPE_PRE_SHUTDOWN)
                {
                    // break out the event handling loop to shutdown syncd
                    runMainLoop = false;
                    break;
                }

                // Handle switch pre-shutdown and wait for the final shutdown
                // event

                SWSS_LOG_TIMER("warm pre-shutdown");

                m_manager->removeAllCounters();

                sai_status_t status = setRestartWarmOnAllSwitches(true);

                if (status != SAI_STATUS_SUCCESS)
                {
                    SWSS_LOG_ERROR("Failed to set SAI_SWITCH_ATTR_RESTART_WARM=true: %s for pre-shutdown",
                            sai_serialize_status(status).c_str());

                    shutdownType = SYNCD_RESTART_TYPE_COLD;

                    warmRestartTable.setFlagFailed();
                    continue;
                }

                status = setPreShutdownOnAllSwitches();

                if (status == SAI_STATUS_SUCCESS)
                {
                    warmRestartTable.setPreShutdown(true);

                    s = std::make_shared<swss::Select>(); // make sure previous select is destroyed

                    s->addSelectable(m_restartQuery.get());

                    SWSS_LOG_NOTICE("switched to PRE_SHUTDOWN, from now on accepting only shutdown requests");
                }
                else
                {
                    SWSS_LOG_ERROR("Failed to set SAI_SWITCH_ATTR_PRE_SHUTDOWN=true: %s",
                            sai_serialize_status(status).c_str());

                    warmRestartTable.setPreShutdown(false);

                    // Restore cold shutdown.

                    setRestartWarmOnAllSwitches(false);
                }
            }
            else if (sel == m_flexCounter.get())
            {
                processFlexCounterEvent(*(swss::ConsumerTable*)sel);
            }
            else if (sel == m_flexCounterGroup.get())
            {
                processFlexCounterGroupEvent(*(swss::ConsumerTable*)sel);
            }
            else if (sel == m_selectableChannel.get())
            {
                processEvent(*m_selectableChannel.get());
            }
            else
            {
                SWSS_LOG_ERROR("select failed: %d", result);
            }

            twd.setEndTime();
        }
        catch(const std::exception &e)
        {
            SWSS_LOG_ERROR("Runtime error: %s", e.what());

            sendShutdownRequestAfterException();

            s = std::make_shared<swss::Select>();

            s->addSelectable(m_restartQuery.get());

            if (m_commandLineOptions->m_disableExitSleep)
                runMainLoop = false;

            // make sure that if second exception will arise, then we break the loop
            m_commandLineOptions->m_disableExitSleep = true;

            twd.setEndTime();
        }
    }

    if (shutdownType == SYNCD_RESTART_TYPE_WARM)
    {
        const char *warmBootWriteFile = profileGetValue(0, SAI_KEY_WARM_BOOT_WRITE_FILE);

        SWSS_LOG_NOTICE("using warmBootWriteFile: '%s'", warmBootWriteFile);

        if (warmBootWriteFile == NULL)
        {
            SWSS_LOG_WARN("user requested warm shutdown but warmBootWriteFile is not specified, forcing cold shutdown");

            shutdownType = SYNCD_RESTART_TYPE_COLD;
            warmRestartTable.setWarmShutdown(false);
        }
        else
        {
            SWSS_LOG_NOTICE("Warm Reboot requested, keeping data plane running");

            sai_status_t status = setRestartWarmOnAllSwitches(true);

            if (status != SAI_STATUS_SUCCESS)
            {
                SWSS_LOG_ERROR("Failed to set SAI_SWITCH_ATTR_RESTART_WARM=true: %s, fall back to cold restart",
                        sai_serialize_status(status).c_str());

                shutdownType = SYNCD_RESTART_TYPE_COLD;

                warmRestartTable.setFlagFailed();
            }
        }
    }

#ifdef SAI_SUPPORT_UNINIT_DATA_PLANE_ON_REMOVAL

    if (shutdownType == SYNCD_RESTART_TYPE_FAST || shutdownType == SYNCD_RESTART_TYPE_WARM)
    {
        setUninitDataPlaneOnRemovalOnAllSwitches();
    }

#endif

    m_manager->removeAllCounters();

    sai_status_t status = removeAllSwitches();

    // Stop notification thread after removing switch
    m_processor->stopNotificationsProcessingThread();

    if (shutdownType == SYNCD_RESTART_TYPE_WARM)
    {
        warmRestartTable.setWarmShutdown(status == SAI_STATUS_SUCCESS);
    }

    SWSS_LOG_NOTICE("calling api uninitialize");

    status = m_vendorSai->uninitialize();

    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_ERROR("failed to uninitialize api: %s", sai_serialize_status(status).c_str());
    }

    SWSS_LOG_NOTICE("uninitialize finished");
}

syncd_restart_type_t Syncd::handleRestartQuery(
        _In_ swss::NotificationConsumer &restartQuery)
{
    SWSS_LOG_ENTER();

    std::string op;
    std::string data;
    std::vector<swss::FieldValueTuple> values;

    restartQuery.pop(op, data, values);

    SWSS_LOG_NOTICE("received %s switch shutdown event", op.c_str());

    return RequestShutdownCommandLineOptions::stringToRestartType(op);
}
