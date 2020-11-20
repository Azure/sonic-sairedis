#include "SwitchStateBase.h"

#include "swss/logger.h"
#include "meta/sai_serialize.h"

#include <net/if.h>

#include <algorithm>

#define MAX_OBJLIST_LEN 128

#define SAI_VS_MAX_PORTS 1024

using namespace saivs;

SwitchStateBase::SwitchStateBase(
        _In_ sai_object_id_t switch_id,
        _In_ std::shared_ptr<RealObjectIdManager> manager,
        _In_ std::shared_ptr<SwitchConfig> config):
    SwitchState(switch_id, config),
    m_realObjectIdManager(manager)
{
    SWSS_LOG_ENTER();

    // empty
}

SwitchStateBase::SwitchStateBase(
        _In_ sai_object_id_t switch_id,
        _In_ std::shared_ptr<RealObjectIdManager> manager,
        _In_ std::shared_ptr<SwitchConfig> config,
        _In_ std::shared_ptr<WarmBootState> warmBootState):
    SwitchState(switch_id, config),
    m_realObjectIdManager(manager)
{
    SWSS_LOG_ENTER();

    if (warmBootState)
    {
        for (auto& kvp: warmBootState->m_objectHash)
        {
            // we write only existing ones, since base constructor
            // created empty entries for non existing object types
            m_objectHash[kvp.first] = kvp.second;
        }

        if (m_switchConfig->m_useTapDevice)
        {
            m_fdb_info_set = warmBootState->m_fdbInfoSet;
        }
    }
}

SwitchStateBase::~SwitchStateBase()
{
    SWSS_LOG_ENTER();

    // empty
}

sai_status_t SwitchStateBase::create(
        _In_ sai_object_type_t object_type,
        _Out_ sai_object_id_t *object_id,
        _In_ sai_object_id_t switch_id,
        _In_ uint32_t attr_count,
        _In_ const sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    if (object_type == SAI_OBJECT_TYPE_SWITCH)
    {
        SWSS_LOG_THROW("this method can't be used to create switch");
    }

    *object_id = m_realObjectIdManager->allocateNewObjectId(object_type, switch_id);

    auto sid = sai_serialize_object_id(*object_id);

    return create_internal(object_type, sid, switch_id, attr_count, attr_list);
}

uint32_t SwitchStateBase::getNewDebugCounterIndex()
{
    SWSS_LOG_ENTER();

    for (uint32_t i = 0; i < maxDebugCounters; i++)
    {
        if (m_indices.find(i) == m_indices.end())
        {
            m_indices.insert(i);
            return i;
        }
    }

    return UINT32_MAX;
}

sai_status_t SwitchStateBase::createDebugCounter(
        _In_ sai_object_id_t object_id,
        _In_ sai_object_id_t switch_id,
        _In_ uint32_t attr_count,
        _In_ const sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    auto index = getNewDebugCounterIndex();

    if (index > maxDebugCounters)
    {
        SWSS_LOG_ERROR("Cannot create more than %d debug counters", maxDebugCounters);

        return SAI_STATUS_FAILURE;
    }

    auto sid = sai_serialize_object_id(object_id);

    CHECK_STATUS(create_internal(SAI_OBJECT_TYPE_DEBUG_COUNTER, sid, switch_id, attr_count, attr_list));

    sai_attribute_t attr;

    attr.id = SAI_DEBUG_COUNTER_ATTR_INDEX;
    attr.value.u32 = index;

    return set(SAI_OBJECT_TYPE_DEBUG_COUNTER, sid, &attr);
}

sai_status_t SwitchStateBase::create(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &serializedObjectId,
        _In_ sai_object_id_t switch_id,
        _In_ uint32_t attr_count,
        _In_ const sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    if (object_type == SAI_OBJECT_TYPE_DEBUG_COUNTER)
    {
        sai_object_id_t object_id;
        sai_deserialize_object_id(serializedObjectId, object_id);
        return createDebugCounter(object_id, switch_id, attr_count, attr_list);
    }

    if (object_type == SAI_OBJECT_TYPE_PORT)
    {
        sai_object_id_t object_id;
        sai_deserialize_object_id(serializedObjectId, object_id);
        return createPort(object_id, switch_id, attr_count, attr_list);
    }

    if (object_type == SAI_OBJECT_TYPE_HOSTIF)
    {
        sai_object_id_t object_id;
        sai_deserialize_object_id(serializedObjectId, object_id);
        return createHostif(object_id, switch_id, attr_count, attr_list);
    }

    return create_internal(object_type, serializedObjectId, switch_id, attr_count, attr_list);
}

sai_status_t SwitchStateBase::create_internal(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &serializedObjectId,
        _In_ sai_object_id_t switch_id,
        _In_ uint32_t attr_count,
        _In_ const sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    auto &objectHash = m_objectHash.at(object_type);

    if (m_switchConfig->m_resourceLimiter)
    {
        size_t limit = m_switchConfig->m_resourceLimiter->getObjectTypeLimit(object_type);

        if (objectHash.size() >= limit)
        {
            SWSS_LOG_ERROR("too many %s, created %zu is resource limit",
                    sai_serialize_object_type(object_type).c_str(),
                    limit);

            return SAI_STATUS_INSUFFICIENT_RESOURCES;
        }
    }

    auto it = objectHash.find(serializedObjectId);

    if (object_type != SAI_OBJECT_TYPE_SWITCH)
    {
        /*
         * Switch is special, and object is already created by init.
         *
         * XXX revisit this.
         */

        if (it != objectHash.end())
        {
            SWSS_LOG_ERROR("create failed, object already exists, object type: %s: id: %s",
                    sai_serialize_object_type(object_type).c_str(),
                    serializedObjectId.c_str());

            return SAI_STATUS_ITEM_ALREADY_EXISTS;
        }
    }

    if (objectHash.find(serializedObjectId) == objectHash.end())
    {
        /*
         * Number of attributes may be zero, so see if actual entry was created
         * with empty hash.
         */

        objectHash[serializedObjectId] = {};
    }

    for (uint32_t i = 0; i < attr_count; ++i)
    {
        auto a = std::make_shared<SaiAttrWrap>(object_type, &attr_list[i]);

        objectHash[serializedObjectId][a->getAttrMetadata()->attridname] = a;
    }

    return SAI_STATUS_SUCCESS;
}

void SwitchStateBase::releaseDebugCounterIndex(
        _In_ uint32_t index)
{
    SWSS_LOG_ENTER();

    m_indices.erase(index);
}

sai_status_t SwitchStateBase::remove(
        _In_ sai_object_type_t object_type,
        _In_ sai_object_id_t objectId)
{
    SWSS_LOG_ENTER();

    auto sid = sai_serialize_object_id(objectId);

    return remove(object_type, sid);
}

sai_status_t SwitchStateBase::removeDebugCounter(
        _In_ sai_object_id_t objectId)
{
    SWSS_LOG_ENTER();

    sai_attribute_t attr;

    attr.id = SAI_DEBUG_COUNTER_ATTR_INDEX;

    CHECK_STATUS(get(SAI_OBJECT_TYPE_DEBUG_COUNTER, objectId, 1, &attr));

    auto sid = sai_serialize_object_id(objectId);

    CHECK_STATUS(remove_internal(SAI_OBJECT_TYPE_DEBUG_COUNTER, sid));

    releaseDebugCounterIndex(attr.value.u32);

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::createPort(
        _In_ sai_object_id_t object_id,
        _In_ sai_object_id_t switch_id,
        _In_ uint32_t attr_count,
        _In_ const sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    auto sid = sai_serialize_object_id(object_id);

    CHECK_STATUS(create_internal(SAI_OBJECT_TYPE_PORT, sid, switch_id, attr_count, attr_list));

    return create_port_dependencies(object_id);
}

sai_status_t SwitchStateBase::removePort(
        _In_ sai_object_id_t objectId)
{
    SWSS_LOG_ENTER();

    if (!isPortReadyToBeRemove(objectId))
    {
        SWSS_LOG_ERROR("port %s still have some active dependencies, can't remove",
                sai_serialize_object_id(objectId).c_str());

        return SAI_STATUS_OBJECT_IN_USE;
    }

    auto dep = getPortDependencies(objectId);

    auto sid = sai_serialize_object_id(objectId);

    CHECK_STATUS(remove_internal(SAI_OBJECT_TYPE_PORT, sid));

    SWSS_LOG_NOTICE("port %s was successfully removed, removing depending objects now",
            sai_serialize_object_id(objectId).c_str());

    for (auto oid: dep)
    {
        // meta_sai_remove_oid automatically removed related oids internally
        // so we just need to execute remove for virtual switch db

        auto status = remove(objectTypeQuery(oid), oid);

        if (status != SAI_STATUS_SUCCESS)
        {
            // we can't continue, there is a bug somewhere if we can't remove
            // port related objects: queues, ipgs, sg

            SWSS_LOG_THROW("FATAL: failed to removed port related oid: %s: %s, bug!",
                    sai_serialize_object_type(objectTypeQuery(oid)).c_str(),
                    sai_serialize_object_id(oid).c_str());
        }
    }

    SWSS_LOG_NOTICE("successfully removed all %zu port related objects", dep.size());

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::remove(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &serializedObjectId)
{
    SWSS_LOG_ENTER();

    if (object_type == SAI_OBJECT_TYPE_DEBUG_COUNTER)
    {
        sai_object_id_t objectId;
        sai_deserialize_object_id(serializedObjectId, objectId);
        return removeDebugCounter(objectId);
    }

    if (object_type == SAI_OBJECT_TYPE_PORT)
    {
        sai_object_id_t objectId;
        sai_deserialize_object_id(serializedObjectId, objectId);
        return removePort(objectId);
    }

    if (object_type == SAI_OBJECT_TYPE_HOSTIF)
    {
        sai_object_id_t objectId;
        sai_deserialize_object_id(serializedObjectId, objectId);
        return removeHostif(objectId);
    }

    return remove_internal(object_type, serializedObjectId);
}

sai_status_t SwitchStateBase::remove_internal(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &serializedObjectId)
{
    SWSS_LOG_ENTER();

    auto &objectHash = m_objectHash.at(object_type);

    auto it = objectHash.find(serializedObjectId);

    if (it == objectHash.end())
    {
        SWSS_LOG_ERROR("not found %s:%s",
                sai_serialize_object_type(object_type).c_str(),
                serializedObjectId.c_str());

        return SAI_STATUS_ITEM_NOT_FOUND;
    }

    objectHash.erase(it);

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::setPort(
        _In_ sai_object_id_t portId,
        _In_ const sai_attribute_t* attr)
{
    SWSS_LOG_ENTER();

    if (attr && attr->id == SAI_PORT_ATTR_ADMIN_STATE && m_switchConfig->m_useTapDevice)
    {
        bool up = attr->value.booldata;

        // find corresponding host if interface and bring it down !
        for (auto& kvp: m_hostif_info_map)
        {
            auto tapname = kvp.first;

            if (kvp.second->m_portId == portId)
            {
                std::string vethname = vs_get_veth_name(tapname, portId);

                if (ifup(vethname.c_str(), portId, up, false))
                {
                    SWSS_LOG_ERROR("if admin %s failed on %s failed: %s", (up ? "UP" : "DOWN"), vethname.c_str(),  strerror(errno));

                    return SAI_STATUS_FAILURE;
                }

                SWSS_LOG_NOTICE("if admin %s success on %s", (up ? "UP" : "DOWN"), vethname.c_str());

                break;
            }
        }
    }
    else if (attr && attr->id == SAI_PORT_ATTR_MTU && m_switchConfig->m_useTapDevice)
    {
        uint32_t mtu = attr->value.u32;

        std::string name;

        if (getTapNameFromPortId(portId, name))
        {
            SWSS_LOG_INFO("setting new MTU: %d on %s", mtu, name.c_str());

            std::string vname = vs_get_veth_name(name, portId);

            if (vs_set_dev_mtu(vname.c_str(), mtu) < 0)
            {
                SWSS_LOG_ERROR("failed to set MTU on portId %s",
                    sai_serialize_object_id(portId).c_str());
                return SAI_STATUS_FAILURE;
            }
        }
    }

    auto sid = sai_serialize_object_id(portId);

    return set_internal(SAI_OBJECT_TYPE_PORT, sid, attr);
}

sai_status_t SwitchStateBase::set(
        _In_ sai_object_type_t objectType,
        _In_ const std::string &serializedObjectId,
        _In_ const sai_attribute_t* attr)
{
    SWSS_LOG_ENTER();

    if (objectType == SAI_OBJECT_TYPE_PORT)
    {
        sai_object_id_t objectId;
        sai_deserialize_object_id(serializedObjectId, objectId);
        return setPort(objectId, attr);
    }

    return set_internal(objectType, serializedObjectId, attr);
}

sai_status_t SwitchStateBase::set_internal(
        _In_ sai_object_type_t objectType,
        _In_ const std::string &serializedObjectId,
        _In_ const sai_attribute_t* attr)
{
    SWSS_LOG_ENTER();

    auto it = m_objectHash.at(objectType).find(serializedObjectId);

    if (it == m_objectHash.at(objectType).end())
    {
        SWSS_LOG_ERROR("not found %s:%s",
                sai_serialize_object_type(objectType).c_str(),
                serializedObjectId.c_str());

        return SAI_STATUS_ITEM_NOT_FOUND;
    }

    auto &attrHash = it->second;

    auto a = std::make_shared<SaiAttrWrap>(objectType, attr);

    // set have only one attribute
    attrHash[a->getAttrMetadata()->attridname] = a;

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::set(
        _In_ sai_object_type_t objectType,
        _In_ sai_object_id_t objectId,
        _In_ const sai_attribute_t* attr)
{
    SWSS_LOG_ENTER();

    auto sid = sai_serialize_object_id(objectId);

    return set(objectType, sid, attr);
}

sai_status_t SwitchStateBase::get(
        _In_ sai_object_type_t object_type,
        _In_ sai_object_id_t object_id,
        _In_ uint32_t attr_count,
        _Out_ sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    auto sid = sai_serialize_object_id(object_id);

    return get(object_type, sid, attr_count, attr_list);
}

sai_status_t SwitchStateBase::get(
        _In_ sai_object_type_t objectType,
        _In_ const std::string &serializedObjectId,
        _In_ uint32_t attr_count,
        _Out_ sai_attribute_t *attr_list)
{
    SWSS_LOG_ENTER();

    const auto &objectHash = m_objectHash.at(objectType);

    auto it = objectHash.find(serializedObjectId);

    if (it == objectHash.end())
    {
        SWSS_LOG_ERROR("not found %s:%s",
                sai_serialize_object_type(objectType).c_str(),
                serializedObjectId.c_str());

        return SAI_STATUS_ITEM_NOT_FOUND;
    }

    /*
     * We need reference here since we can potentially update attr hash for RO
     * object.
     */

    auto& attrHash = it->second;

    /*
     * Some of the list query maybe for length, so we can't do
     * normal serialize, maybe with count only.
     */

    sai_status_t final_status = SAI_STATUS_SUCCESS;

    for (uint32_t idx = 0; idx < attr_count; ++idx)
    {
        sai_attr_id_t id = attr_list[idx].id;

        auto meta = sai_metadata_get_attr_metadata(objectType, id);

        if (meta == NULL)
        {
            SWSS_LOG_ERROR("failed to find attribute %d for %s:%s", id,
                    sai_serialize_object_type(objectType).c_str(),
                    serializedObjectId.c_str());

            return SAI_STATUS_FAILURE;
        }

        sai_status_t status;

        if (SAI_HAS_FLAG_READ_ONLY(meta->flags))
        {
            /*
             * Read only attributes may require recalculation.
             * Metadata makes sure that non object id's can't have
             * read only attributes. So here is definitely OID.
             */

            sai_object_id_t oid;
            sai_deserialize_object_id(serializedObjectId, oid);

            status = refresh_read_only(meta, oid);

            if (status != SAI_STATUS_SUCCESS)
            {
                SWSS_LOG_INFO("%s read only not implemented on %s",
                        meta->attridname,
                        serializedObjectId.c_str());

                return status;
            }
        }

        auto ait = attrHash.find(meta->attridname);

        if (ait == attrHash.end())
        {
            SWSS_LOG_INFO("%s not implemented on %s",
                    meta->attridname,
                    serializedObjectId.c_str());

            return SAI_STATUS_NOT_IMPLEMENTED;
        }

        auto attr = ait->second->getAttr();

        status = transfer_attributes(objectType, 1, attr, &attr_list[idx], false);

        if (status == SAI_STATUS_BUFFER_OVERFLOW)
        {
            /*
             * This is considered partial success, since we get correct list
             * length.  Note that other items ARE processes on the list.
             */

            SWSS_LOG_NOTICE("BUFFER_OVERFLOW %s: %s",
                    serializedObjectId.c_str(),
                    meta->attridname);

            /*
             * We still continue processing other attributes for get as long as
             * we only will be getting buffer overflow error.
             */

            final_status = status;
            continue;
        }

        if (status != SAI_STATUS_SUCCESS)
        {
            // all other errors

            SWSS_LOG_ERROR("get failed %s: %s: %s",
                    serializedObjectId.c_str(),
                    meta->attridname,
                    sai_serialize_status(status).c_str());

            return status;
        }
    }

    return final_status;
}

sai_status_t SwitchStateBase::bulkCreate(
        _In_ sai_object_id_t switch_id,
        _In_ sai_object_type_t object_type,
        _In_ const std::vector<std::string> &serialized_object_ids,
        _In_ const uint32_t *attr_count,
        _In_ const sai_attribute_t **attr_list,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    SWSS_LOG_ENTER();

    uint32_t object_count = (uint32_t) serialized_object_ids.size();

    if (!object_count || !attr_count || !attr_list || !object_statuses)
    {
        SWSS_LOG_ERROR("Invalid arguments");
        return SAI_STATUS_FAILURE;
    }

    sai_status_t status = SAI_STATUS_SUCCESS;
    uint32_t it;

    for (it = 0; it < object_count; it++)
    {
        object_statuses[it] = create_internal(object_type, serialized_object_ids[it], switch_id, attr_count[it], attr_list[it]);

        if (object_statuses[it] != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_ERROR("Failed to create object with type = %u", object_type);

            status = SAI_STATUS_FAILURE;

            if (mode == SAI_BULK_OP_ERROR_MODE_STOP_ON_ERROR)
            {
                break;
            }
        }
    }

    while (++it < object_count)
    {
        object_statuses[it] = SAI_STATUS_NOT_EXECUTED;
    }

    return status;
}

sai_status_t SwitchStateBase::bulkRemove(
        _In_ sai_object_type_t object_type,
        _In_ const std::vector<std::string> &serialized_object_ids,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    SWSS_LOG_ENTER();

    uint32_t object_count = (uint32_t) serialized_object_ids.size();

    if (!object_count || !object_statuses)
    {
        SWSS_LOG_ERROR("Invalid arguments");
        return SAI_STATUS_FAILURE;
    }

    sai_status_t status = SAI_STATUS_SUCCESS;
    uint32_t it;

    for (it = 0; it < object_count; it++)
    {
        object_statuses[it] = remove_internal(object_type, serialized_object_ids[it]);

        if (object_statuses[it] != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_ERROR("Failed to remove object with type = %u", object_type);

            status = SAI_STATUS_FAILURE;

            if (mode == SAI_BULK_OP_ERROR_MODE_STOP_ON_ERROR)
            {
                break;
            }
        }
    }

    while (++it < object_count)
    {
        object_statuses[it] = SAI_STATUS_NOT_EXECUTED;
    }

    return status;
}

sai_status_t SwitchStateBase::bulkSet(
        _In_ sai_object_type_t object_type,
        _In_ const std::vector<std::string> &serialized_object_ids,
        _In_ const sai_attribute_t *attr_list,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    SWSS_LOG_ENTER();

    uint32_t object_count = (uint32_t) serialized_object_ids.size();

    if (!object_count || !attr_list || !object_statuses)
    {
        SWSS_LOG_ERROR("Invalid arguments");
        return SAI_STATUS_FAILURE;
    }

    sai_status_t status = SAI_STATUS_SUCCESS;
    uint32_t it;

    for (it = 0; it < object_count; it++)
    {
        object_statuses[it] = set_internal(object_type, serialized_object_ids[it], &attr_list[it]);

        if (object_statuses[it] != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_ERROR("Failed to set attribute for object with type = %u", object_type);

            status = SAI_STATUS_FAILURE;

            if (mode == SAI_BULK_OP_ERROR_MODE_STOP_ON_ERROR)
            {
                break;
            }
        }
    }

    while (++it < object_count)
    {
        object_statuses[it] = SAI_STATUS_NOT_EXECUTED;
    }

    return status;
}

static int get_default_gw_mac_address(
        _Out_ sai_mac_t& mac)
{
    SWSS_LOG_ENTER();

    auto file = std::ifstream("/proc/net/route");

    if (!file)
    {
        return -1;
    }

    std::string buf;

    while (std::getline(file, buf))
    {
        char iface[IF_NAMESIZE];

        long destination, gateway;

        if (std::sscanf(buf.c_str(), "%s %lx %lx", iface, &destination, &gateway) == 3)
        {
            if (destination == 0)
            {
                file = std::ifstream("/sys/class/net/" + std::string(iface) + "/address");

                if ( !file )
                {
                    return -1;
                }

                file >> buf;

                return std::sscanf(buf.c_str(), "%hhx:%hhx:%hhx:%hhx:%hhx:%hhx", &mac[0], &mac[1], &mac[2], &mac[3], &mac[4], &mac[5]) == 6 ? 0 : -1;
            }
        }
    }

    return -1;
}

sai_status_t SwitchStateBase::set_switch_mac_address()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("create switch src mac address");

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_SRC_MAC_ADDRESS;

    if (get_default_gw_mac_address(attr.value.mac) < 0)
    {
        attr.value.mac[0] = 0x22;
        attr.value.mac[1] = 0x33;
        attr.value.mac[2] = 0x44;
        attr.value.mac[3] = 0x55;
        attr.value.mac[4] = 0x66;
        attr.value.mac[5] = 0x77;
    }

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::set_switch_default_attributes()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("create switch default attributes");

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_PORT_STATE_CHANGE_NOTIFY;
    attr.value.ptr = NULL;

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    attr.id = SAI_SWITCH_ATTR_FDB_EVENT_NOTIFY;

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    attr.id = SAI_SWITCH_ATTR_FDB_AGING_TIME;
    attr.value.u32 = 0;

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    attr.id = SAI_SWITCH_ATTR_RESTART_WARM;
    attr.value.booldata = false;

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    attr.id = SAI_SWITCH_ATTR_TYPE;
    attr.value.s32 = SAI_SWITCH_TYPE_NPU;

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    attr.id = SAI_SWITCH_ATTR_WARM_RECOVER;
    attr.value.booldata = false;

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::set_static_crm_values()
{
    SWSS_LOG_ENTER();

    std::map<sai_switch_attr_t, int> crm_resource_lookup = {
        { SAI_SWITCH_ATTR_AVAILABLE_IPV4_ROUTE_ENTRY, m_maxIPv4RouteEntries },
        { SAI_SWITCH_ATTR_AVAILABLE_IPV6_ROUTE_ENTRY, m_maxIPv6RouteEntries },
        { SAI_SWITCH_ATTR_AVAILABLE_IPV4_NEXTHOP_ENTRY, m_maxIPv4NextHopEntries },
        { SAI_SWITCH_ATTR_AVAILABLE_IPV6_NEXTHOP_ENTRY, m_maxIPv6NextHopEntries },
        { SAI_SWITCH_ATTR_AVAILABLE_IPV4_NEIGHBOR_ENTRY, m_maxIPv4NeighborEntries },
        { SAI_SWITCH_ATTR_AVAILABLE_IPV6_NEIGHBOR_ENTRY, m_maxIPv6NeighborEntries },
        { SAI_SWITCH_ATTR_AVAILABLE_NEXT_HOP_GROUP_MEMBER_ENTRY, m_maxNextHopGroupMemberEntries },
        { SAI_SWITCH_ATTR_AVAILABLE_NEXT_HOP_GROUP_ENTRY, m_maxNextHopGroupEntries },
        { SAI_SWITCH_ATTR_AVAILABLE_FDB_ENTRY, m_maxFdbEntries },
        { SAI_SWITCH_ATTR_AVAILABLE_SNAT_ENTRY, m_maxSNATEntries },
        { SAI_SWITCH_ATTR_AVAILABLE_DNAT_ENTRY, m_maxDNATEntries },
        { SAI_SWITCH_ATTR_AVAILABLE_DOUBLE_NAT_ENTRY, m_maxDoubleNATEntries }
    };

    for (auto const &resource: crm_resource_lookup)
    {
        sai_attribute_t attr;
        attr.id = resource.first;
        attr.value.u32 = resource.second;
        CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));
    }

    CHECK_STATUS(set_static_acl_resource_list(SAI_SWITCH_ATTR_AVAILABLE_ACL_TABLE, m_maxAclTables));

    return set_static_acl_resource_list(SAI_SWITCH_ATTR_AVAILABLE_ACL_TABLE_GROUP, m_maxAclTableGroups);
}

sai_status_t SwitchStateBase::set_static_acl_resource_list(
        _In_ sai_switch_attr_t acl_resource,
        _In_ int max_count)
{
    auto acl_stages = {SAI_ACL_STAGE_INGRESS, SAI_ACL_STAGE_EGRESS};
    auto acl_bind_points = {
        SAI_ACL_BIND_POINT_TYPE_PORT,
        SAI_ACL_BIND_POINT_TYPE_LAG,
        SAI_ACL_BIND_POINT_TYPE_VLAN,
        SAI_ACL_BIND_POINT_TYPE_ROUTER_INTERFACE,
        SAI_ACL_BIND_POINT_TYPE_SWITCH
    };

    std::vector<sai_acl_resource_t> acl_resource_list;
    for (auto stage: acl_stages)
    {
        for (auto bp: acl_bind_points)
        {
            sai_acl_resource_t acl_resource_count;
            acl_resource_count.stage = stage;
            acl_resource_count.bind_point = bp;
            acl_resource_count.avail_num = max_count;

            acl_resource_list.push_back(acl_resource_count);
        }
    }

    sai_acl_resource_list_t available_acl_resources;
    available_acl_resources.count = static_cast<uint32_t>(acl_resource_list.size());
    available_acl_resources.list = acl_resource_list.data();

    sai_attribute_t attr;
    attr.id = acl_resource;
    attr.value.aclresource = available_acl_resources;

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::create_default_vlan()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("create default vlan");

    sai_attribute_t attr;

    attr.id = SAI_VLAN_ATTR_VLAN_ID;
    attr.value.u16 = DEFAULT_VLAN_NUMBER;

    CHECK_STATUS(create(SAI_OBJECT_TYPE_VLAN, &m_default_vlan_id, m_switch_id, 1, &attr));

    /* set default vlan on switch */

    attr.id = SAI_SWITCH_ATTR_DEFAULT_VLAN_ID;
    attr.value.oid = m_default_vlan_id;

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::create_cpu_port()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("create cpu port");

    sai_attribute_t attr;

    CHECK_STATUS(create(SAI_OBJECT_TYPE_PORT, &m_cpu_port_id, m_switch_id, 0, &attr));

    // populate cpu port object on switch
    attr.id = SAI_SWITCH_ATTR_CPU_PORT;
    attr.value.oid = m_cpu_port_id;

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    // set type on cpu
    attr.id = SAI_PORT_ATTR_TYPE;
    attr.value.s32 = SAI_PORT_TYPE_CPU;

    return set(SAI_OBJECT_TYPE_PORT, m_cpu_port_id, &attr);
}

sai_status_t SwitchStateBase::create_default_1q_bridge()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("create default 1q bridge");

    sai_attribute_t attr;

    attr.id = SAI_BRIDGE_ATTR_TYPE;
    attr.value.s32 = SAI_BRIDGE_TYPE_1Q;

    CHECK_STATUS(create(SAI_OBJECT_TYPE_BRIDGE, &m_default_1q_bridge, m_switch_id, 1, &attr));

    attr.id = SAI_SWITCH_ATTR_DEFAULT_1Q_BRIDGE_ID;
    attr.value.oid = m_default_1q_bridge;

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::create_ports()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("create ports");

    auto map = m_switchConfig->m_laneMap;

    if (!map)
    {
        SWSS_LOG_ERROR("lane map for switch %s is NULL",
                sai_serialize_object_id(m_switch_id).c_str());

        return SAI_STATUS_FAILURE;
    }

    auto& lanesVector = map->getLaneVector();

    uint32_t port_count = (uint32_t)lanesVector.size();

    m_port_list.clear();

    for (uint32_t i = 0; i < port_count; i++)
    {
        SWSS_LOG_DEBUG("create port index %u", i);

        sai_object_id_t port_id;

        CHECK_STATUS(create(SAI_OBJECT_TYPE_PORT, &port_id, m_switch_id, 0, NULL));
        m_port_list.push_back(port_id);

        sai_attribute_t attr;

        attr.id = SAI_PORT_ATTR_ADMIN_STATE;
        attr.value.booldata = false;     /* default admin state is down as defined in SAI */

        CHECK_STATUS(set(SAI_OBJECT_TYPE_PORT, port_id, &attr));

        attr.id = SAI_PORT_ATTR_MTU;
        attr.value.u32 = 1514;     /* default MTU is 1514 as defined in SAI */

        CHECK_STATUS(set(SAI_OBJECT_TYPE_PORT, port_id, &attr));

        attr.id = SAI_PORT_ATTR_SPEED;
        attr.value.u32 = 40 * 1000;     // TODO from config

        CHECK_STATUS(set(SAI_OBJECT_TYPE_PORT, port_id, &attr));

        std::vector<uint32_t> lanes = lanesVector.at(i);

        attr.id = SAI_PORT_ATTR_HW_LANE_LIST;
        attr.value.u32list.count = (uint32_t)lanes.size();
        attr.value.u32list.list = lanes.data();

        CHECK_STATUS(set(SAI_OBJECT_TYPE_PORT, port_id, &attr));

        attr.id = SAI_PORT_ATTR_TYPE;
        attr.value.s32 = SAI_PORT_TYPE_LOGICAL;

        CHECK_STATUS(set(SAI_OBJECT_TYPE_PORT, port_id, &attr));

        attr.id = SAI_PORT_ATTR_OPER_STATUS;
        attr.value.s32 = SAI_PORT_OPER_STATUS_DOWN;

        CHECK_STATUS(set(SAI_OBJECT_TYPE_PORT, port_id, &attr));

        attr.id = SAI_PORT_ATTR_PORT_VLAN_ID;
        attr.value.u32 = DEFAULT_VLAN_NUMBER;

        CHECK_STATUS(set(SAI_OBJECT_TYPE_PORT, port_id, &attr));
    }

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::set_port_list()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("set port list");

    // NOTE: this is static, but will be refreshed on read only get

    sai_attribute_t attr;

    uint32_t port_count = (uint32_t)m_port_list.size();

    attr.id = SAI_SWITCH_ATTR_PORT_LIST;
    attr.value.objlist.count = port_count;
    attr.value.objlist.list = m_port_list.data();

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    attr.id = SAI_SWITCH_ATTR_PORT_NUMBER;
    attr.value.u32 = port_count;

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::create_default_virtual_router()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("create default virtual router");

    sai_object_id_t virtual_router_id;

    CHECK_STATUS(create(SAI_OBJECT_TYPE_VIRTUAL_ROUTER, &virtual_router_id, m_switch_id, 0, NULL));

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_DEFAULT_VIRTUAL_ROUTER_ID;
    attr.value.oid = virtual_router_id;

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::create_default_stp_instance()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("create default stp instance");

    sai_object_id_t stp_instance_id;

    CHECK_STATUS(create(SAI_OBJECT_TYPE_STP, &stp_instance_id, m_switch_id, 0, NULL));

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_DEFAULT_STP_INST_ID;
    attr.value.oid = stp_instance_id;

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::create_default_trap_group()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("create default trap group");

    sai_object_id_t trap_group_id;

    CHECK_STATUS(create(SAI_OBJECT_TYPE_HOSTIF_TRAP_GROUP, &trap_group_id, m_switch_id, 0, NULL));

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_DEFAULT_TRAP_GROUP;
    attr.value.oid = trap_group_id;

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::create_ingress_priority_groups_per_port(
        _In_ sai_object_id_t port_id)
{
    SWSS_LOG_ENTER();

    const uint32_t port_pgs_count = 8; // NOTE: must be per switch (mlnx and brcm is 8)

    std::vector<sai_object_id_t> pgs;

    for (uint32_t i = 0; i < port_pgs_count; ++i)
    {
        sai_object_id_t pg_id;

        sai_attribute_t attr[3];

        // NOTE: on brcm this attribute is not added

        attr[0].id = SAI_INGRESS_PRIORITY_GROUP_ATTR_BUFFER_PROFILE;
        attr[0].value.oid = SAI_NULL_OBJECT_ID;

        attr[1].id = SAI_INGRESS_PRIORITY_GROUP_ATTR_PORT;
        attr[1].value.oid = port_id;

        attr[2].id = SAI_INGRESS_PRIORITY_GROUP_ATTR_INDEX;
        attr[2].value.oid = i;

        CHECK_STATUS(create(SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP, &pg_id, m_switch_id, 3, attr));

        pgs.push_back(pg_id);
    }

    sai_attribute_t attr;

    attr.id = SAI_PORT_ATTR_NUMBER_OF_INGRESS_PRIORITY_GROUPS;
    attr.value.u32 = port_pgs_count;

    CHECK_STATUS(set(SAI_OBJECT_TYPE_PORT, port_id, &attr));

    attr.id = SAI_PORT_ATTR_INGRESS_PRIORITY_GROUP_LIST;
    attr.value.objlist.count = port_pgs_count;
    attr.value.objlist.list = pgs.data();

    CHECK_STATUS(set(SAI_OBJECT_TYPE_PORT, port_id, &attr));

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::create_ingress_priority_groups()
{
    SWSS_LOG_ENTER();

    // XXX priority groups size may change when we will modify pg or ports

    SWSS_LOG_INFO("create ingress priority groups");

    for (auto &port_id : m_port_list)
    {
        create_ingress_priority_groups_per_port(port_id);
    }

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::create_vlan_members()
{
    SWSS_LOG_ENTER();

    // Crete vlan members for bridge ports.

    for (auto bridge_port_id: m_bridge_port_list_port_based)
    {
        SWSS_LOG_DEBUG("create vlan member for bridge port %s",
                sai_serialize_object_id(bridge_port_id).c_str());

        sai_attribute_t attrs[3];

        attrs[0].id = SAI_VLAN_MEMBER_ATTR_BRIDGE_PORT_ID;
        attrs[0].value.oid = bridge_port_id;

        attrs[1].id = SAI_VLAN_MEMBER_ATTR_VLAN_ID;
        attrs[1].value.oid = m_default_vlan_id;

        attrs[2].id = SAI_VLAN_MEMBER_ATTR_VLAN_TAGGING_MODE;
        attrs[2].value.s32 = SAI_VLAN_TAGGING_MODE_UNTAGGED;

        sai_object_id_t vlan_member_id;

        CHECK_STATUS(create(SAI_OBJECT_TYPE_VLAN_MEMBER, &vlan_member_id, m_switch_id, 3, attrs));
    }

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::create_bridge_ports()
{
    SWSS_LOG_ENTER();

    // Create bridge port for 1q router.

    sai_attribute_t attr;

    attr.id = SAI_BRIDGE_PORT_ATTR_TYPE;
    attr.value.s32 = SAI_BRIDGE_PORT_TYPE_1Q_ROUTER;

    CHECK_STATUS(create(SAI_OBJECT_TYPE_BRIDGE_PORT, &m_default_bridge_port_1q_router, m_switch_id, 1, &attr));

    attr.id = SAI_BRIDGE_PORT_ATTR_PORT_ID;
    attr.value.oid = SAI_NULL_OBJECT_ID;

    CHECK_STATUS(set(SAI_OBJECT_TYPE_BRIDGE_PORT, m_default_bridge_port_1q_router, &attr));

    // Create bridge ports for regular ports.

    m_bridge_port_list_port_based.clear();

    for (const auto &port_id: m_port_list)
    {
        SWSS_LOG_DEBUG("create bridge port for port %s", sai_serialize_object_id(port_id).c_str());

        sai_attribute_t attrs[4];

        attrs[0].id = SAI_BRIDGE_PORT_ATTR_BRIDGE_ID;
        attrs[0].value.oid = m_default_1q_bridge;

        attrs[1].id = SAI_BRIDGE_PORT_ATTR_FDB_LEARNING_MODE;
        attrs[1].value.s32 = SAI_BRIDGE_PORT_FDB_LEARNING_MODE_HW;

        attrs[2].id = SAI_BRIDGE_PORT_ATTR_PORT_ID;
        attrs[2].value.oid = port_id;

        attrs[3].id = SAI_BRIDGE_PORT_ATTR_TYPE;
        attrs[3].value.s32 = SAI_BRIDGE_PORT_TYPE_PORT;

        sai_object_id_t bridge_port_id;

        CHECK_STATUS(create(SAI_OBJECT_TYPE_BRIDGE_PORT, &bridge_port_id, m_switch_id, 4, attrs));

        m_bridge_port_list_port_based.push_back(bridge_port_id);
    }

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::set_acl_entry_min_prio()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("set acl entry min prio");

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_ACL_ENTRY_MINIMUM_PRIORITY;
    attr.value.u32 = 1;

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    attr.id = SAI_SWITCH_ATTR_ACL_ENTRY_MAXIMUM_PRIORITY;
    attr.value.u32 = 16000;

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::set_acl_capabilities()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("set acl capabilities");

    sai_attribute_t attr;

    m_ingress_acl_action_list.clear();
    m_egress_acl_action_list.clear();

    for (int action_type = SAI_ACL_ENTRY_ATTR_ACTION_START; action_type <= SAI_ACL_ENTRY_ATTR_ACTION_END; action_type++)
    {
        m_ingress_acl_action_list.push_back(static_cast<sai_acl_action_type_t>(action_type - SAI_ACL_ENTRY_ATTR_ACTION_START));
        m_egress_acl_action_list.push_back(static_cast<sai_acl_action_type_t>(action_type - SAI_ACL_ENTRY_ATTR_ACTION_START));
    }

    attr.id = SAI_SWITCH_ATTR_MAX_ACL_ACTION_COUNT;
    attr.value.u32 = static_cast<uint32_t>(std::max(m_ingress_acl_action_list.size(), m_egress_acl_action_list.size()));

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    attr.id = SAI_SWITCH_ATTR_ACL_STAGE_INGRESS;
    attr.value.aclcapability.action_list.list = reinterpret_cast<int32_t*>(m_ingress_acl_action_list.data());
    attr.value.aclcapability.action_list.count = static_cast<uint32_t>(m_ingress_acl_action_list.size());

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    attr.id = SAI_SWITCH_ATTR_ACL_STAGE_EGRESS;
    attr.value.aclcapability.action_list.list = reinterpret_cast<int32_t*>(m_egress_acl_action_list.data());
    attr.value.aclcapability.action_list.count = static_cast<uint32_t>(m_egress_acl_action_list.size());

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::create_qos_queues_per_port(
        _In_ sai_object_id_t port_id)
{
    SWSS_LOG_ENTER();

    SWSS_LOG_ERROR("implement in child class");

    return SAI_STATUS_NOT_IMPLEMENTED;
}

sai_status_t SwitchStateBase::create_qos_queues()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_ERROR("implement in child class");

    return SAI_STATUS_NOT_IMPLEMENTED;
}

sai_status_t SwitchStateBase::create_scheduler_group_tree(
        _In_ const std::vector<sai_object_id_t>& sgs,
        _In_ sai_object_id_t port_id)
{
    SWSS_LOG_ENTER();

    SWSS_LOG_ERROR("implement in child class");

    return SAI_STATUS_NOT_IMPLEMENTED;
}

sai_status_t SwitchStateBase::create_scheduler_groups_per_port(
        _In_ sai_object_id_t port_id)
{
    SWSS_LOG_ENTER();

    SWSS_LOG_ERROR("implement in child class");

    return SAI_STATUS_NOT_IMPLEMENTED;
}

sai_status_t SwitchStateBase::create_scheduler_groups()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("create scheduler groups");

    // XXX scheduler groups size may change when we will modify sg or ports

    for (auto &port_id : m_port_list)
    {
        CHECK_STATUS(create_scheduler_groups_per_port(port_id));
    }

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::set_maximum_number_of_childs_per_scheduler_group()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_ERROR("implement in child class");

    return SAI_STATUS_NOT_IMPLEMENTED;
}

sai_status_t SwitchStateBase::set_number_of_ecmp_groups()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("set number of ecmp groups");

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_NUMBER_OF_ECMP_GROUPS;
    attr.value.u32 = 512;

    return set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr);
}

sai_status_t SwitchStateBase::initialize_default_objects()
{
    SWSS_LOG_ENTER();

    CHECK_STATUS(set_switch_mac_address());
    CHECK_STATUS(create_cpu_port());
    CHECK_STATUS(create_default_vlan());
    CHECK_STATUS(create_default_virtual_router());
    CHECK_STATUS(create_default_stp_instance());
    CHECK_STATUS(create_default_1q_bridge());
    CHECK_STATUS(create_default_trap_group());
    CHECK_STATUS(create_ports());
    CHECK_STATUS(set_port_list());
    CHECK_STATUS(create_bridge_ports());
    CHECK_STATUS(create_vlan_members());
    CHECK_STATUS(set_acl_entry_min_prio());
    CHECK_STATUS(set_acl_capabilities());
    CHECK_STATUS(create_ingress_priority_groups());
    CHECK_STATUS(create_qos_queues());
    CHECK_STATUS(set_maximum_number_of_childs_per_scheduler_group());
    CHECK_STATUS(set_number_of_ecmp_groups());
    CHECK_STATUS(set_switch_default_attributes());
    CHECK_STATUS(create_scheduler_groups());
    CHECK_STATUS(set_static_crm_values());

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::create_port_dependencies(
        _In_ sai_object_id_t port_id)
{
    SWSS_LOG_ENTER();

    // params are not needed since they were already set

    SWSS_LOG_WARN("check attributes and set, FIXME");

    // this method is post create action on generic create object

    sai_attribute_t attr;

    // default admin state is down as defined in SAI

    attr.id = SAI_PORT_ATTR_ADMIN_STATE;
    attr.value.booldata = false;

    CHECK_STATUS(set(SAI_OBJECT_TYPE_PORT, port_id, &attr));

    // attributes are not required since they will be set outside this function

    CHECK_STATUS(create_ingress_priority_groups_per_port(port_id));
    CHECK_STATUS(create_qos_queues_per_port(port_id));
    CHECK_STATUS(create_scheduler_groups_per_port(port_id));

    // XXX should bridge ports should also be created when new port is created?
    // this needs to be checked on real ASIC and updated here

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::refresh_ingress_priority_group(
        _In_ const sai_attr_metadata_t *meta,
        _In_ sai_object_id_t port_id)
{
    SWSS_LOG_ENTER();

    /*
     * TODO Currently we don't have index in groups, so we don't know how to
     * sort.  Returning success, since assuming that we will not create more
     * ingress priority groups.
     */

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::refresh_qos_queues(
        _In_ const sai_attr_metadata_t *meta,
        _In_ sai_object_id_t port_id)
{
    SWSS_LOG_ENTER();

    /*
     * TODO Currently we don't have index in groups, so we don't know how to
     * sort.  Returning success, since assuming that we will not create more
     * ingress priority groups.
     */

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::refresh_scheduler_groups(
        _In_ const sai_attr_metadata_t *meta,
        _In_ sai_object_id_t port_id)
{
    SWSS_LOG_ENTER();

    /*
     * TODO Currently we don't have index in groups, so we don't know how to
     * sort.  Returning success, since assuming that we will not create more
     * ingress priority groups.
     */

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::warm_boot_initialize_objects()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_INFO("warm boot init objects");

    /*
     * We need to bring back previous state in case user will get some read
     * only attributes and recalculation will need to be done.
     *
     * We need to refresh:
     * - ports
     * - default bridge port 1q router
     */

    m_port_list.resize(SAI_VS_MAX_PORTS);

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_PORT_LIST;

    attr.value.objlist.count = SAI_VS_MAX_PORTS;
    attr.value.objlist.list = m_port_list.data();

    CHECK_STATUS(get(SAI_OBJECT_TYPE_SWITCH, m_switch_id, 1, &attr));

    m_port_list.resize(attr.value.objlist.count);

    SWSS_LOG_NOTICE("port list size: %zu", m_port_list.size());

    attr.id = SAI_SWITCH_ATTR_DEFAULT_1Q_BRIDGE_ID;

    CHECK_STATUS(get(SAI_OBJECT_TYPE_SWITCH, m_switch_id, 1, &attr));

    m_default_bridge_port_1q_router = attr.value.oid;

    SWSS_LOG_NOTICE("default bridge port 1q router: %s",
            sai_serialize_object_id(m_default_bridge_port_1q_router).c_str());

    // TODO refresh
    //
    // m_bridge_port_list_port_based;
    // m_cpu_port_id;
    // m_default_1q_bridge;
    // m_default_vlan_id;
    //
    // other objects/numbers if added (per switch also)

    // TODO update
    // std::unordered_set<uint32_t> m_indices;

    return SAI_STATUS_SUCCESS;
}

sai_status_t SwitchStateBase::refresh_bridge_port_list(
        _In_ const sai_attr_metadata_t *meta,
        _In_ sai_object_id_t bridge_id)
{
    SWSS_LOG_ENTER();

    SWSS_LOG_ERROR("implement in child class");

    return SAI_STATUS_NOT_IMPLEMENTED;
}

sai_status_t SwitchStateBase::refresh_vlan_member_list(
        _In_ const sai_attr_metadata_t *meta,
        _In_ sai_object_id_t vlan_id)
{
    SWSS_LOG_ENTER();

    auto &all_vlan_members = m_objectHash.at(SAI_OBJECT_TYPE_VLAN_MEMBER);

    auto m_member_list = sai_metadata_get_attr_metadata(SAI_OBJECT_TYPE_VLAN, SAI_VLAN_ATTR_MEMBER_LIST);
    auto md_vlan_id = sai_metadata_get_attr_metadata(SAI_OBJECT_TYPE_VLAN_MEMBER, SAI_VLAN_MEMBER_ATTR_VLAN_ID);
    //auto md_brportid = sai_metadata_get_attr_metadata(SAI_OBJECT_TYPE_VLAN_MEMBER, SAI_VLAN_MEMBER_ATTR_BRIDGE_PORT_ID);

    std::vector<sai_object_id_t> vlan_member_list;

    /*
     * We want order as bridge port order (so port order)
     */

    sai_attribute_t attr;

    auto me = m_objectHash.at(SAI_OBJECT_TYPE_VLAN).at(sai_serialize_object_id(vlan_id));

    for (auto vm: all_vlan_members)
    {
        if (vm.second.at(md_vlan_id->attridname)->getAttr()->value.oid != vlan_id)
        {
            /*
             * Only interested in our vlan
             */

            continue;
        }

        // TODO we need order as bridge ports, but we need bridge id!

        {
            sai_object_id_t vlan_member_id;

            sai_deserialize_object_id(vm.first, vlan_member_id);

            vlan_member_list.push_back(vlan_member_id);
        }
    }

    uint32_t vlan_member_list_count = (uint32_t)vlan_member_list.size();

    SWSS_LOG_NOTICE("recalculated %s: %u", m_member_list->attridname, vlan_member_list_count);

    attr.id = SAI_VLAN_ATTR_MEMBER_LIST;
    attr.value.objlist.count = vlan_member_list_count;
    attr.value.objlist.list = vlan_member_list.data();

    return set(SAI_OBJECT_TYPE_VLAN, vlan_id, &attr);
}

sai_status_t SwitchStateBase::refresh_port_list(
        _In_ const sai_attr_metadata_t *meta)
{
    SWSS_LOG_ENTER();

    // since now port can be added or removed, we need to update port list
    // dynamically

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_CPU_PORT;

    CHECK_STATUS(get(SAI_OBJECT_TYPE_SWITCH, m_switch_id, 1, &attr));

    const sai_object_id_t cpu_port_id = attr.value.oid;

    m_port_list.clear();

    // iterate via ASIC state to find all the ports

    for (const auto& it: m_objectHash.at(SAI_OBJECT_TYPE_PORT))
    {
        sai_object_id_t port_id;
        sai_deserialize_object_id(it.first, port_id);

        // don't put CPU port id on the list

        if (port_id == cpu_port_id)
            continue;

        m_port_list.push_back(port_id);
    }

    /*
     * XXX Currently we don't know what's happen on brcm SAI implementation
     * when port is removed and then added, will new port could get the same
     * vendor OID or always different, and what is order of those new oids on
     * the PORT_LIST attribute.
     *
     * This needs to be investigated, and to reflect exact behaviour here.
     * Currently we just sort all the port oids.
     */

    std::sort(m_port_list.begin(), m_port_list.end());

    uint32_t port_count = (uint32_t)m_port_list.size();

    attr.id = SAI_SWITCH_ATTR_PORT_LIST;
    attr.value.objlist.count = port_count;
    attr.value.objlist.list = m_port_list.data();

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    attr.id = SAI_SWITCH_ATTR_PORT_NUMBER;
    attr.value.u32 = port_count;

    CHECK_STATUS(set(SAI_OBJECT_TYPE_SWITCH, m_switch_id, &attr));

    SWSS_LOG_NOTICE("refreshed port list, current port number: %zu, not counting cpu port", m_port_list.size());

    return SAI_STATUS_SUCCESS;
}

// XXX extra work may be needed on GET api if N on list will be > then actual

/*
 * We can use local variable here for initialization (init should be in class
 * constructor anyway, we can move it there later) because each switch init is
 * done under global lock.
 */

/*
 * NOTE For recalculation we can add flag on create/remove specific object type
 * so we can deduce whether actually need to perform recalculation, as
 * optimization.
 */

sai_status_t SwitchStateBase::refresh_read_only(
        _In_ const sai_attr_metadata_t *meta,
        _In_ sai_object_id_t object_id)
{
    SWSS_LOG_ENTER();

    if (meta->objecttype == SAI_OBJECT_TYPE_SWITCH)
    {
        switch (meta->attrid)
        {
            case SAI_SWITCH_ATTR_CPU_PORT:
            case SAI_SWITCH_ATTR_DEFAULT_VIRTUAL_ROUTER_ID:
            case SAI_SWITCH_ATTR_DEFAULT_TRAP_GROUP:
            case SAI_SWITCH_ATTR_DEFAULT_VLAN_ID:
            case SAI_SWITCH_ATTR_DEFAULT_STP_INST_ID:
            case SAI_SWITCH_ATTR_DEFAULT_1Q_BRIDGE_ID:
                return SAI_STATUS_SUCCESS;

            case SAI_SWITCH_ATTR_ACL_ENTRY_MINIMUM_PRIORITY:
            case SAI_SWITCH_ATTR_ACL_ENTRY_MAXIMUM_PRIORITY:
                return SAI_STATUS_SUCCESS;

            case SAI_SWITCH_ATTR_MAX_ACL_ACTION_COUNT:
            case SAI_SWITCH_ATTR_ACL_STAGE_INGRESS:
            case SAI_SWITCH_ATTR_ACL_STAGE_EGRESS:
                return SAI_STATUS_SUCCESS;

            case SAI_SWITCH_ATTR_NUMBER_OF_ECMP_GROUPS:
                return SAI_STATUS_SUCCESS;

            case SAI_SWITCH_ATTR_PORT_NUMBER:
            case SAI_SWITCH_ATTR_PORT_LIST:
                return refresh_port_list(meta);

            case SAI_SWITCH_ATTR_QOS_MAX_NUMBER_OF_CHILDS_PER_SCHEDULER_GROUP:
                return SAI_STATUS_SUCCESS;

            case SAI_SWITCH_ATTR_AVAILABLE_IPV4_ROUTE_ENTRY:
            case SAI_SWITCH_ATTR_AVAILABLE_IPV6_ROUTE_ENTRY:
            case SAI_SWITCH_ATTR_AVAILABLE_IPV4_NEXTHOP_ENTRY:
            case SAI_SWITCH_ATTR_AVAILABLE_IPV6_NEXTHOP_ENTRY:
            case SAI_SWITCH_ATTR_AVAILABLE_IPV4_NEIGHBOR_ENTRY:
            case SAI_SWITCH_ATTR_AVAILABLE_IPV6_NEIGHBOR_ENTRY:
            case SAI_SWITCH_ATTR_AVAILABLE_NEXT_HOP_GROUP_MEMBER_ENTRY:
            case SAI_SWITCH_ATTR_AVAILABLE_NEXT_HOP_GROUP_ENTRY:
            case SAI_SWITCH_ATTR_AVAILABLE_FDB_ENTRY:
            case SAI_SWITCH_ATTR_AVAILABLE_ACL_TABLE:
            case SAI_SWITCH_ATTR_AVAILABLE_ACL_TABLE_GROUP:
            case SAI_SWITCH_ATTR_AVAILABLE_SNAT_ENTRY:
            case SAI_SWITCH_ATTR_AVAILABLE_DNAT_ENTRY:
            case SAI_SWITCH_ATTR_AVAILABLE_DOUBLE_NAT_ENTRY:
                return SAI_STATUS_SUCCESS;
        }
    }

    if (meta->objecttype == SAI_OBJECT_TYPE_PORT)
    {
        switch (meta->attrid)
        {
            case SAI_PORT_ATTR_QOS_NUMBER_OF_QUEUES:
            case SAI_PORT_ATTR_QOS_QUEUE_LIST:
                return refresh_qos_queues(meta, object_id);

            case SAI_PORT_ATTR_NUMBER_OF_INGRESS_PRIORITY_GROUPS:
            case SAI_PORT_ATTR_INGRESS_PRIORITY_GROUP_LIST:
                return refresh_ingress_priority_group(meta, object_id);

            case SAI_PORT_ATTR_QOS_NUMBER_OF_SCHEDULER_GROUPS:
            case SAI_PORT_ATTR_QOS_SCHEDULER_GROUP_LIST:
                return refresh_scheduler_groups(meta, object_id);

                /*
                 * This status is based on hostif vEthernetX status.
                 */

            case SAI_PORT_ATTR_OPER_STATUS:
                return SAI_STATUS_SUCCESS;
        }
    }

    if (meta->objecttype == SAI_OBJECT_TYPE_SCHEDULER_GROUP)
    {
        switch (meta->attrid)
        {
            case SAI_SCHEDULER_GROUP_ATTR_CHILD_COUNT:
            case SAI_SCHEDULER_GROUP_ATTR_CHILD_LIST:
                return refresh_scheduler_groups(meta, object_id);
        }
    }

    if (meta->objecttype == SAI_OBJECT_TYPE_BRIDGE && meta->attrid == SAI_BRIDGE_ATTR_PORT_LIST)
    {
        return refresh_bridge_port_list(meta, object_id);
    }

    if (meta->objecttype == SAI_OBJECT_TYPE_VLAN && meta->attrid == SAI_VLAN_ATTR_MEMBER_LIST)
    {
        return refresh_vlan_member_list(meta, object_id);
    }

    if (meta->objecttype == SAI_OBJECT_TYPE_DEBUG_COUNTER && meta->attrid == SAI_DEBUG_COUNTER_ATTR_INDEX)
    {
        return SAI_STATUS_SUCCESS;
    }

    auto mmeta = m_meta.lock();

    if (mmeta)
    {
        if (mmeta->meta_unittests_enabled())
        {
            SWSS_LOG_NOTICE("unittests enabled, SET could be performed on %s, not recalculating", meta->attridname);

            return SAI_STATUS_SUCCESS;
        }
    }
    else
    {
        SWSS_LOG_WARN("meta pointer expired");
    }

    SWSS_LOG_WARN("need to recalculate RO: %s", meta->attridname);

    return SAI_STATUS_NOT_IMPLEMENTED;
}

void SwitchStateBase::processFdbEntriesForAging()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_DEBUG("fdb infos to process: %zu", m_fdb_info_set.size());

    uint32_t current = (uint32_t)time(NULL);

    sai_attribute_t attr;

    attr.id = SAI_SWITCH_ATTR_FDB_AGING_TIME;

    sai_status_t status = get(SAI_OBJECT_TYPE_SWITCH, m_switch_id, 1, &attr);

    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_WARN("failed to get FDB aging time for switch %s",
                sai_serialize_object_id(m_switch_id).c_str());

        return;
    }

    uint32_t aging_time = attr.value.u32;

    if (aging_time == 0)
    {
        SWSS_LOG_DEBUG("aging is disabled");
        return;
    }

    // find aged fdb entries

    for (auto it = m_fdb_info_set.begin(); it != m_fdb_info_set.end();)
    {
        if ((current - it->getTimestamp()) >= aging_time)
        {
            FdbInfo fi = *it;

            processFdbInfo(fi, SAI_FDB_EVENT_AGED);

            it = m_fdb_info_set.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

bool SwitchStateBase::isPortReadyToBeRemove(
        _In_ sai_object_id_t portId)
{
    SWSS_LOG_ENTER();

    std::vector<sai_object_id_t> dep;

    auto status = check_port_dependencies(portId, dep);

    return status == SAI_STATUS_SUCCESS;
}

std::vector<sai_object_id_t> SwitchStateBase::getPortDependencies(
        _In_ sai_object_id_t portId)
{
    SWSS_LOG_ENTER();

    std::vector<sai_object_id_t> dep;

    check_port_dependencies(portId, dep);

    return dep;
}

bool SwitchStateBase::check_port_reference_count(
        _In_ sai_object_id_t port_id)
{
    SWSS_LOG_ENTER();

    // TODO currently when switch is initialized, there is no metadata yet
    // and objects are created without reference count, this needs to be
    // addressed in refactoring metadata and meta_create_oid to correct
    // count references, but now we need to check if port is used in any
    // bridge port (and bridge port in any vlan member), after metadata
    // refactor this function can be removed.

    // check if port is used on any bridge port object (only switch init one
    // matters, user created bridge ports will have correct reference count

    auto& bridgePorts = m_objectHash.at(SAI_OBJECT_TYPE_BRIDGE_PORT);

    auto* meta = sai_metadata_get_attr_metadata(SAI_OBJECT_TYPE_BRIDGE_PORT, SAI_BRIDGE_PORT_ATTR_PORT_ID);

    for (auto& bp: bridgePorts)
    {
        for (auto&attr: bp.second)
        {
            if (attr.first != meta->attridname)
                continue; // not this attribute

            if (attr.second->getAttr()->value.oid == port_id)
            {
                SWSS_LOG_ERROR("port id %s is in use on bridge port %s",
                        sai_serialize_object_id(port_id).c_str(),
                        bp.first.c_str());

                return false;
            }
        }
    }

    return true;
}

sai_status_t SwitchStateBase::check_port_dependencies(
        _In_ sai_object_id_t port_id,
        _Out_ std::vector<sai_object_id_t>& dep)
{
    SWSS_LOG_ENTER();

    // check if port exists's

    sai_object_id_t switch_id = switchIdQuery(port_id);

    if (switch_id == SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_ERROR("failed to obtain switch_id from object %s",
                sai_serialize_object_id(port_id).c_str());

        return SAI_STATUS_FAILURE;
    }

    sai_object_type_t ot = objectTypeQuery(port_id);

    if (ot != SAI_OBJECT_TYPE_PORT)
    {
        SWSS_LOG_ERROR("expected object type PORT but object %s has type %s",
                sai_serialize_object_id(port_id).c_str(),
                sai_serialize_object_type(ot).c_str());

        return SAI_STATUS_FAILURE;
    }

    std::string str_port_id = sai_serialize_object_id(port_id);

    auto &objectHash = m_objectHash.at(ot);

    auto it = objectHash.find(str_port_id);

    if (it == objectHash.end())
    {
        SWSS_LOG_ERROR("port not found %s:%s",
                sai_serialize_object_type(ot).c_str(),
                str_port_id.c_str());

        return SAI_STATUS_ITEM_NOT_FOUND;
    }

    // port was found
    SWSS_LOG_NOTICE("port %s found, for removal",
                sai_serialize_object_id(port_id).c_str());

    // check port reference count on bridge port

    if (!check_port_reference_count(port_id))
    {
        SWSS_LOG_ERROR("port %s reference count IS NOT ZERO, can't remove, remove dependencies first",
                sai_serialize_object_id(port_id).c_str());

        return SAI_STATUS_FAILURE;
    }


    // obtain objects to examine

    std::vector<sai_object_id_t> queues;
    std::vector<sai_object_id_t> ipgs;
    std::vector<sai_object_id_t> sg;

    bool result = true;

    result &= get_port_queues(port_id, queues);
    result &= get_port_ipgs(port_id, ipgs);
    result &= get_port_sg(port_id, sg);

    if (!result)
    {
        SWSS_LOG_ERROR("failed to obtain required objects on port %s",
                sai_serialize_object_id(port_id).c_str());

        return SAI_STATUS_FAILURE;
    }

    // check if all objects are in default state

    result &= check_object_default_state(port_id);
    result &= check_object_list_default_state(queues);
    result &= check_object_list_default_state(ipgs);
    result &= check_object_list_default_state(sg);

    if (!result)
    {
        SWSS_LOG_ERROR("one of objects is not in default state, can't remove port %s",
                sai_serialize_object_id(port_id).c_str());

        return SAI_STATUS_FAILURE;
    }

    SWSS_LOG_NOTICE("all depending objects on port %s are in default state",
                sai_serialize_object_id(port_id).c_str());

    dep.insert(dep.end(), queues.begin(), queues.end());
    dep.insert(dep.end(), ipgs.begin(), ipgs.end());
    dep.insert(dep.end(), sg.begin(), sg.end());

    // BRIDGE PORT (and also VLAN MEMBER using that bridge port) must be
    // removed before removing port itself, since bridge port holds reference
    // to port being removed.

    // bridge ports and vlan members must be removed before port can be removed

    return SAI_STATUS_SUCCESS;
}


bool SwitchStateBase::get_object_list(
        _In_ sai_object_id_t object_id,
        _In_ sai_attr_id_t attr_id,
        _Out_ std::vector<sai_object_id_t>& objlist)
{
    SWSS_LOG_ENTER();

    objlist.clear();

    sai_object_type_t object_type = objectTypeQuery(object_id);

    auto* meta = sai_metadata_get_attr_metadata(object_type, attr_id);

    if (meta == nullptr)
    {
        SWSS_LOG_THROW("failed to get metadata for OID %s and attrid: %d",
                sai_serialize_object_id(object_id).c_str(),
                attr_id);
    }

    if (meta->attrvaluetype != SAI_ATTR_VALUE_TYPE_OBJECT_LIST)
    {
        SWSS_LOG_THROW("attr %s is not objlist attribute", meta->attridname);
    }

    sai_status_t status;

    sai_attribute_t attr;

    objlist.resize(MAX_OBJLIST_LEN);

    attr.id = attr_id;

    attr.value.objlist.count = MAX_OBJLIST_LEN;
    attr.value.objlist.list = objlist.data();

    status = get(object_type, object_id, 1, &attr);

    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_ERROR("failed to obtain %s for %s queues: %s",
                meta->attridname,
                sai_serialize_object_id(object_id).c_str(),
                sai_serialize_status(status).c_str());

        objlist.clear();
        return false;
    }

    objlist.resize(attr.value.objlist.count);

    SWSS_LOG_NOTICE("%s returned %zu objects for %s",
            meta->attridname,
            objlist.size(),
            sai_serialize_object_id(object_id).c_str());

    return true;
}

bool SwitchStateBase::get_port_queues(
        _In_ sai_object_id_t port_id,
        _Out_ std::vector<sai_object_id_t>& queues)
{
    SWSS_LOG_ENTER();

    return get_object_list(port_id, SAI_PORT_ATTR_QOS_QUEUE_LIST, queues);
}

bool SwitchStateBase::get_port_ipgs(
        _In_ sai_object_id_t port_id,
        _Out_ std::vector<sai_object_id_t>& ipgs)
{
    SWSS_LOG_ENTER();

    return get_object_list(port_id, SAI_PORT_ATTR_INGRESS_PRIORITY_GROUP_LIST, ipgs);
}

bool SwitchStateBase::get_port_sg(
        _In_ sai_object_id_t port_id,
        _Out_ std::vector<sai_object_id_t>& sg)
{
    SWSS_LOG_ENTER();

    // scheduler groups are organized in tree, but
    // SAI_PORT_ATTR_INGRESS_PRIORITY_GROUP_LIST should return all scheduler groups in that tree

    return get_object_list(port_id, SAI_PORT_ATTR_QOS_SCHEDULER_GROUP_LIST, sg);
}

bool SwitchStateBase::check_object_default_state(
        _In_ sai_object_id_t object_id)
{
    SWSS_LOG_ENTER();

    sai_object_type_t object_type = objectTypeQuery(object_id);

    if (object_type == SAI_OBJECT_TYPE_NULL)
    {
        SWSS_LOG_ERROR("failed to get object type for oid: %s",
                sai_serialize_object_id(object_id).c_str());

        return false;
    }

    auto* oti = sai_metadata_get_object_type_info(object_type);

    if (oti == nullptr)
    {
        SWSS_LOG_THROW("failed to get object type info for object type: %s",
                sai_serialize_object_type(object_type).c_str());
    }

    // iterate over all attributes

    for (size_t i = 0; i < oti->attrmetadatalength; i++)
    {
        auto* meta = oti->attrmetadata[i];

        // skip readonly, mandatory on create and non oid attributes

        if (meta->isreadonly)
            continue;

        if (!meta->isoidattribute)
            continue;

        // those attributes must be skipped since those dependencies will be automatically broken
        if (meta->objecttype == SAI_OBJECT_TYPE_SCHEDULER_GROUP && meta->attrid == SAI_SCHEDULER_GROUP_ATTR_PORT_ID)
            continue;

        if (meta->objecttype == SAI_OBJECT_TYPE_SCHEDULER_GROUP && meta->attrid == SAI_SCHEDULER_GROUP_ATTR_PARENT_NODE)
            continue;

        if (meta->objecttype == SAI_OBJECT_TYPE_QUEUE && meta->attrid == SAI_QUEUE_ATTR_PORT)
            continue;

        if (meta->objecttype == SAI_OBJECT_TYPE_QUEUE && meta->attrid == SAI_QUEUE_ATTR_PARENT_SCHEDULER_NODE)
            continue;

        if (meta->objecttype == SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP && meta->attrid == SAI_INGRESS_PRIORITY_GROUP_ATTR_PORT)
            continue;

        // here we have only oid/object list attrs and we expect each of this
        // attribute will be in default state which for oid is usually NULL,
        // and for object list is empty

        sai_attribute_t attr;

        attr.id = meta->attrid;

        sai_status_t status;

        std::vector<sai_object_id_t> objlist;

        if (meta->attrvaluetype == SAI_ATTR_VALUE_TYPE_OBJECT_ID)
        {
            // ok
        }
        else if (meta->attrvaluetype == SAI_ATTR_VALUE_TYPE_OBJECT_LIST)
        {
            objlist.resize(MAX_OBJLIST_LEN);

            attr.value.objlist.count = MAX_OBJLIST_LEN;
            attr.value.objlist.list = objlist.data();
        }
        else
        {
            // unable to check whether object is in default state, need fix

            SWSS_LOG_ERROR("unsupported oid attribute: %s, FIX ME!", meta->attridname);
            return false;
        }

        status = get(object_type, object_id, 1, &attr);

        switch (status)
        {
            case SAI_STATUS_NOT_IMPLEMENTED:
            case SAI_STATUS_NOT_SUPPORTED:
                continue;

            case SAI_STATUS_SUCCESS:
                break;

            default:

                SWSS_LOG_ERROR("unexpected status %s on %s obj %s",
                        sai_serialize_status(status).c_str(),
                        meta->attridname,
                        sai_serialize_object_id(object_id).c_str());
                return false;

        }


        if (meta->attrvaluetype == SAI_ATTR_VALUE_TYPE_OBJECT_ID)
        {
            if (attr.value.oid != SAI_NULL_OBJECT_ID)
            {
                SWSS_LOG_ERROR("expected null object id on %s on %s, but got: %s",
                        meta->attridname,
                        sai_serialize_object_id(object_id).c_str(),
                        sai_serialize_object_id(attr.value.oid).c_str());

                return false;
            }

        }
        else if (meta->attrvaluetype == SAI_ATTR_VALUE_TYPE_OBJECT_LIST)
        {
            if (objlist.size())
            {
                SWSS_LOG_ERROR("expected empty list on %s on %s, contents:",
                        meta->attridname,
                        sai_serialize_object_id(object_id).c_str());

                for (auto oid: objlist)
                {
                    SWSS_LOG_ERROR(" - oid: %s", sai_serialize_object_id(oid).c_str());
                }

                return false;
            }
        }
        else
        {
            // unable to check whether object is in default state, need fix

            SWSS_LOG_ERROR("unsupported oid attribute: %s, FIX ME!", meta->attridname);
            return false;
        }
    }

    // XXX later there can be issue when we for example add extra queues to
    // the port those new queues should be removed by user first before
    // removing port, and currently we don't have a way to differentiate those

    // object is in default state
    return true;
}

bool SwitchStateBase::check_object_list_default_state(
        _Out_ const std::vector<sai_object_id_t>& objlist)
{
    SWSS_LOG_ENTER();

    return std::all_of(objlist.begin(), objlist.end(),
            [&](sai_object_id_t oid) { return check_object_default_state(oid); });
}

std::string SwitchStateBase::dump_switch_database_for_warm_restart() const
{
    SWSS_LOG_ENTER();

    std::stringstream ss;

    const auto& objectHash = m_objectHash;

    // dump all objects and attributes to file

    size_t count = 0;

    for (auto kvp: objectHash)
    {
        auto singleTypeObjectMap = kvp.second;

        count += singleTypeObjectMap.size();

        for (auto o: singleTypeObjectMap)
        {
            // if object don't have attributes, size can be zero
            if (o.second.size() == 0)
            {
                ss << sai_serialize_object_type(kvp.first) << " " << o.first << " NULL NULL" << std::endl;
                continue;
            }

            for (auto a: o.second)
            {
                ss << sai_serialize_object_type(kvp.first) << " ";
                ss << o.first.c_str();
                ss << " ";
                ss << a.first.c_str();
                ss << " ";
                ss << a.second->getAttrStrValue();
                ss << std::endl;
            }
        }
    }

    if (m_switchConfig->m_useTapDevice)
    {
        /*
         * If user is using tap devices we also need to dump local fdb info
         * data and restore it on warm start.
         */

        for (auto fi: m_fdb_info_set)
        {
            ss << SAI_VS_FDB_INFO << " " << fi.serialize() << std::endl;
        }

        SWSS_LOG_NOTICE("dumped %zu fdb infos for switch %s",
                m_fdb_info_set.size(),
                sai_serialize_object_id(m_switch_id).c_str());
    }

    SWSS_LOG_NOTICE("dumped %zu objects from switch %s",
            count,
            sai_serialize_object_id(m_switch_id).c_str());

    return ss.str();
}

sai_object_type_t SwitchStateBase::objectTypeQuery(
        _In_ sai_object_id_t objectId)
{
    SWSS_LOG_ENTER();

    return RealObjectIdManager::objectTypeQuery(objectId);
}

sai_object_id_t SwitchStateBase::switchIdQuery(
        _In_ sai_object_id_t objectId)
{
    SWSS_LOG_ENTER();

    return RealObjectIdManager::switchIdQuery(objectId);
}

void SwitchStateBase::debugSetStats(
        _In_ sai_object_id_t oid,
        _In_ const std::map<sai_stat_id_t, uint64_t>& stats)
{
    SWSS_LOG_ENTER();

    auto key = sai_serialize_object_id(oid);

    for (auto& kvp: stats)
    {
        m_countersMap[key][kvp.first] = kvp.second;
    }
}

void SwitchStateBase::findObjects(
        _In_ sai_object_type_t object_type,
        _In_ const sai_attribute_t &expect,
        _Out_ std::vector<sai_object_id_t> &objects)
{
    SWSS_LOG_ENTER();

    objects.clear();

    SaiAttrWrap expect_wrap(object_type, &expect);

    for (auto &obj : m_objectHash.at(object_type))
    {
        auto attr_itr = obj.second.find(expect_wrap.getAttrMetadata()->attridname);

        if (attr_itr != obj.second.end()
                && attr_itr->second->getAttrStrValue() == expect_wrap.getAttrStrValue())
        {
            sai_object_id_t object_id;
            sai_deserialize_object_id(obj.first, object_id);
            objects.push_back(object_id);
        }
    }
}

bool SwitchStateBase::dumpObject(
        _In_ const sai_object_id_t object_id,
        _Out_ std::vector<sai_attribute_t> &attrs)
{
    SWSS_LOG_ENTER();

    attrs.clear();

    auto &objs = m_objectHash.at(objectTypeQuery(object_id));
    auto obj = objs.find(sai_serialize_object_id(object_id));

    if (obj == objs.end())
    {
        return false;
    }

    for (auto &attr : obj->second)
    {
        attrs.push_back(*attr.second->getAttr());
    }

    return true;
}

