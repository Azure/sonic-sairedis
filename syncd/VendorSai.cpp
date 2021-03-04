#include "VendorSai.h"

#include "meta/sai_serialize.h"

#include "swss/logger.h"

#include <cstring>

using namespace syncd;

#define MUTEX() std::lock_guard<std::mutex> _lock(m_apimutex)

#define VENDOR_CHECK_API_INITIALIZED()                                       \
    if (!m_apiInitialized) {                                                \
        SWSS_LOG_ERROR("%s: api not initialized", __PRETTY_FUNCTION__);     \
        return SAI_STATUS_FAILURE; }

VendorSai::VendorSai()
{
    SWSS_LOG_ENTER();

    m_apiInitialized = false;

    memset(&m_apis, 0, sizeof(m_apis));
}

VendorSai::~VendorSai()
{
    SWSS_LOG_ENTER();

    if (m_apiInitialized)
    {
        uninitialize();
    }
}

// INITIALIZE UNINITIALIZE

sai_status_t VendorSai::initialize(
        _In_ uint64_t flags,
        _In_ const sai_service_method_table_t *service_method_table)
{
    MUTEX();
    SWSS_LOG_ENTER();

    if (m_apiInitialized)
    {
        SWSS_LOG_ERROR("%s: api already initialized", __PRETTY_FUNCTION__);

        return SAI_STATUS_FAILURE;
    }

    if ((service_method_table == NULL))
    {
        SWSS_LOG_ERROR("invalid service_method_table handle passed to SAI API initialize");

        return SAI_STATUS_INVALID_PARAMETER;
    }

    memcpy(&m_service_method_table, service_method_table, sizeof(m_service_method_table));

    auto status = sai_api_initialize(flags, service_method_table);

    if (status == SAI_STATUS_SUCCESS)
    {
        memset(&m_apis, 0, sizeof(m_apis));

        int failed = sai_metadata_apis_query(sai_api_query, &m_apis);

        if (failed > 0)
        {
            SWSS_LOG_NOTICE("sai_api_query failed for %d apis", failed);
        }

        m_apiInitialized = true;
    }

    return status;
}

sai_status_t VendorSai::uninitialize(void)
{
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    auto status = sai_api_uninitialize();

    if (status == SAI_STATUS_SUCCESS)
    {
        m_apiInitialized = false;

        memset(&m_apis, 0, sizeof(m_apis));
    }

    return status;
}

// QUAD OID

sai_status_t VendorSai::create(
        _In_ sai_object_type_t objectType,
        _Out_ sai_object_id_t* objectId,
        _In_ sai_object_id_t switchId,
        _In_ uint32_t attr_count,
        _In_ const sai_attribute_t *attr_list)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    auto info = sai_metadata_get_object_type_info(objectType);

    if (!info)
    {
        SWSS_LOG_ERROR("unable to get info for object type: %s",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    if (!info->create)
    {
        SWSS_LOG_ERROR("object type %s has no create method",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    if (info->isnonobjectid)
    {
        SWSS_LOG_ERROR("passed non object id as object id!: %s",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    sai_object_meta_key_t mk = { .objecttype = objectType, .objectkey = { .key = { .object_id = 0 } } };

    auto status = info->create(&mk, switchId, attr_count, attr_list);

    if (status == SAI_STATUS_SUCCESS)
    {
        *objectId = mk.objectkey.key.object_id;
    }

    return status;
}

sai_status_t VendorSai::remove(
        _In_ sai_object_type_t objectType,
        _In_ sai_object_id_t objectId)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    auto info = sai_metadata_get_object_type_info(objectType);

    if (!info)
    {
        SWSS_LOG_ERROR("unable to get info for object type: %s",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    if (!info->remove)
    {
        SWSS_LOG_ERROR("object type %s has no remove method",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    if (info->isnonobjectid)
    {
        SWSS_LOG_ERROR("passed non object id as object id!: %s",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    sai_object_meta_key_t mk = { .objecttype = objectType, .objectkey = { .key = { .object_id = objectId } } };

    return info->remove(&mk);
}

sai_status_t VendorSai::set(
        _In_ sai_object_type_t objectType,
        _In_ sai_object_id_t objectId,
        _In_ const sai_attribute_t *attr)
{
    std::unique_lock<std::mutex> _lock(m_apimutex);
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    auto info = sai_metadata_get_object_type_info(objectType);

    if (!info)
    {
        SWSS_LOG_ERROR("unable to get info for object type: %s",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    if (!info->set)
    {
        SWSS_LOG_ERROR("object type %s has no set method",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    if (info->isnonobjectid)
    {
        SWSS_LOG_ERROR("passed non object id as object id!: %s",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    sai_object_meta_key_t mk = { .objecttype = objectType, .objectkey = { .key = { .object_id = objectId } } };

    if (objectType == SAI_OBJECT_TYPE_SWITCH && attr && attr->id == SAI_SWITCH_ATTR_SWITCH_SHELL_ENABLE)
    {
        // in case of diagnostic shell, this vendor api can be blocking, so
        // release lock here to not cause deadlock for other events in syncd
        _lock.unlock();
    }

    return info->set(&mk, attr);
}

sai_status_t VendorSai::get(
        _In_ sai_object_type_t objectType,
        _In_ sai_object_id_t objectId,
        _In_ uint32_t attr_count,
        _Inout_ sai_attribute_t *attr_list)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    auto info = sai_metadata_get_object_type_info(objectType);

    if (!info)
    {
        SWSS_LOG_ERROR("unable to get info for object type: %s",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    if (!info->get)
    {
        SWSS_LOG_ERROR("object type %s has no get method",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    if (info->isnonobjectid)
    {
        SWSS_LOG_ERROR("passed non object id as object id!: %s",
                sai_serialize_object_type(objectType).c_str());

        return SAI_STATUS_FAILURE;
    }

    sai_object_meta_key_t mk = { .objecttype = objectType, .objectkey = { .key = { .object_id = objectId } } };

    return info->get(&mk, attr_count, attr_list);
}

// QUAD ENTRY

#define DECLARE_CREATE_ENTRY(OT,ot)                                         \
sai_status_t VendorSai::create(                                             \
        _In_ const sai_ ## ot ## _t* entry,                                 \
        _In_ uint32_t attr_count,                                           \
        _In_ const sai_attribute_t *attr_list)                              \
{                                                                           \
    MUTEX();                                                                \
    SWSS_LOG_ENTER();                                                       \
    VENDOR_CHECK_API_INITIALIZED();                                         \
    auto info = sai_metadata_get_object_type_info(SAI_OBJECT_TYPE_ ## OT);  \
    sai_object_meta_key_t mk = { .objecttype = info->objecttype,            \
        .objectkey = { .key = { .ot = *entry } } };                         \
    return info->create(&mk, 0, attr_count, attr_list);                     \
}

DECLARE_CREATE_ENTRY(FDB_ENTRY,fdb_entry);
DECLARE_CREATE_ENTRY(INSEG_ENTRY,inseg_entry);
DECLARE_CREATE_ENTRY(IPMC_ENTRY,ipmc_entry);
DECLARE_CREATE_ENTRY(L2MC_ENTRY,l2mc_entry);
DECLARE_CREATE_ENTRY(MCAST_FDB_ENTRY,mcast_fdb_entry);
DECLARE_CREATE_ENTRY(NEIGHBOR_ENTRY,neighbor_entry);
DECLARE_CREATE_ENTRY(ROUTE_ENTRY,route_entry);
DECLARE_CREATE_ENTRY(NAT_ENTRY,nat_entry);


#define DECLARE_REMOVE_ENTRY(OT,ot)                                         \
sai_status_t VendorSai::remove(                                             \
        _In_ const sai_ ## ot ## _t* entry)                                 \
{                                                                           \
    MUTEX();                                                                \
    SWSS_LOG_ENTER();                                                       \
    VENDOR_CHECK_API_INITIALIZED();                                         \
    auto info = sai_metadata_get_object_type_info(SAI_OBJECT_TYPE_ ## OT);  \
    sai_object_meta_key_t mk = { .objecttype = info->objecttype,            \
        .objectkey = { .key = { .ot = *entry } } };                         \
    return info->remove(&mk);                                               \
}

DECLARE_REMOVE_ENTRY(FDB_ENTRY,fdb_entry);
DECLARE_REMOVE_ENTRY(INSEG_ENTRY,inseg_entry);
DECLARE_REMOVE_ENTRY(IPMC_ENTRY,ipmc_entry);
DECLARE_REMOVE_ENTRY(L2MC_ENTRY,l2mc_entry);
DECLARE_REMOVE_ENTRY(MCAST_FDB_ENTRY,mcast_fdb_entry);
DECLARE_REMOVE_ENTRY(NEIGHBOR_ENTRY,neighbor_entry);
DECLARE_REMOVE_ENTRY(ROUTE_ENTRY,route_entry);
DECLARE_REMOVE_ENTRY(NAT_ENTRY,nat_entry);

#define DECLARE_SET_ENTRY(OT,ot)                                            \
sai_status_t VendorSai::set(                                                \
        _In_ const sai_ ## ot ## _t* entry,                                 \
        _In_ const sai_attribute_t *attr)                                   \
{                                                                           \
    MUTEX();                                                                \
    SWSS_LOG_ENTER();                                                       \
    VENDOR_CHECK_API_INITIALIZED();                                         \
    auto info = sai_metadata_get_object_type_info(SAI_OBJECT_TYPE_ ## OT);  \
    sai_object_meta_key_t mk = { .objecttype = info->objecttype,            \
        .objectkey = { .key = { .ot = *entry } } };                         \
    return info->set(&mk, attr);                                            \
}

DECLARE_SET_ENTRY(FDB_ENTRY,fdb_entry);
DECLARE_SET_ENTRY(INSEG_ENTRY,inseg_entry);
DECLARE_SET_ENTRY(IPMC_ENTRY,ipmc_entry);
DECLARE_SET_ENTRY(L2MC_ENTRY,l2mc_entry);
DECLARE_SET_ENTRY(MCAST_FDB_ENTRY,mcast_fdb_entry);
DECLARE_SET_ENTRY(NEIGHBOR_ENTRY,neighbor_entry);
DECLARE_SET_ENTRY(ROUTE_ENTRY,route_entry);
DECLARE_SET_ENTRY(NAT_ENTRY,nat_entry);

#define DECLARE_GET_ENTRY(OT,ot)                                            \
sai_status_t VendorSai::get(                                                \
        _In_ const sai_ ## ot ## _t* entry,                                 \
        _In_ uint32_t attr_count,                                           \
        _Inout_ sai_attribute_t *attr_list)                                 \
{                                                                           \
    MUTEX();                                                                \
    SWSS_LOG_ENTER();                                                       \
    VENDOR_CHECK_API_INITIALIZED();                                         \
    auto info = sai_metadata_get_object_type_info(SAI_OBJECT_TYPE_ ## OT);  \
    sai_object_meta_key_t mk = { .objecttype = info->objecttype,            \
        .objectkey = { .key = { .ot = *entry } } };                         \
    return info->get(&mk, attr_count, attr_list);                           \
}

DECLARE_GET_ENTRY(FDB_ENTRY,fdb_entry);
DECLARE_GET_ENTRY(INSEG_ENTRY,inseg_entry);
DECLARE_GET_ENTRY(IPMC_ENTRY,ipmc_entry);
DECLARE_GET_ENTRY(L2MC_ENTRY,l2mc_entry);
DECLARE_GET_ENTRY(MCAST_FDB_ENTRY,mcast_fdb_entry);
DECLARE_GET_ENTRY(NEIGHBOR_ENTRY,neighbor_entry);
DECLARE_GET_ENTRY(ROUTE_ENTRY,route_entry);
DECLARE_GET_ENTRY(NAT_ENTRY,nat_entry);

// STATS

sai_status_t VendorSai::getStats(
        _In_ sai_object_type_t object_type,
        _In_ sai_object_id_t object_id,
        _In_ uint32_t number_of_counters,
        _In_ const sai_stat_id_t *counter_ids,
        _Out_ uint64_t *counters)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    sai_status_t (*ptr)(
            _In_ sai_object_id_t port_id,
            _In_ uint32_t number_of_counters,
            _In_ const sai_stat_id_t *counter_ids,
            _Out_ uint64_t *counters);

    if (!counter_ids || !counters)
    {
        SWSS_LOG_ERROR("NULL pointer function argument");
        return SAI_STATUS_INVALID_PARAMETER;
    }

    switch ((int)object_type)
    {
        case SAI_OBJECT_TYPE_PORT:
            ptr = m_apis.port_api->get_port_stats;
            break;
        case SAI_OBJECT_TYPE_ROUTER_INTERFACE:
            ptr = m_apis.router_interface_api->get_router_interface_stats;
            break;
        case SAI_OBJECT_TYPE_POLICER:
            ptr = m_apis.policer_api->get_policer_stats;
            break;
        case SAI_OBJECT_TYPE_QUEUE:
            ptr = m_apis.queue_api->get_queue_stats;
            break;
        case SAI_OBJECT_TYPE_BUFFER_POOL:
            ptr = m_apis.buffer_api->get_buffer_pool_stats;
            break;
        case SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP:
            ptr = m_apis.buffer_api->get_ingress_priority_group_stats;
            break;
        case SAI_OBJECT_TYPE_SWITCH:
            ptr = m_apis.switch_api->get_switch_stats;
            break;
        case SAI_OBJECT_TYPE_VLAN:
            ptr = m_apis.vlan_api->get_vlan_stats;
            break;
        case SAI_OBJECT_TYPE_TUNNEL:
            ptr = m_apis.tunnel_api->get_tunnel_stats;
            break;
        case SAI_OBJECT_TYPE_BRIDGE:
            ptr = m_apis.bridge_api->get_bridge_stats;
            break;
        case SAI_OBJECT_TYPE_BRIDGE_PORT:
            ptr = m_apis.bridge_api->get_bridge_port_stats;
            break;
        case SAI_OBJECT_TYPE_PORT_POOL:
            ptr = m_apis.port_api->get_port_pool_stats;
            break;
        case SAI_OBJECT_TYPE_BFD_SESSION:
            ptr = m_apis.bfd_api->get_bfd_session_stats;
            break;
        case SAI_OBJECT_TYPE_COUNTER:
            ptr = m_apis.counter_api->get_counter_stats;
            break;
        case SAI_OBJECT_TYPE_TABLE_BITMAP_CLASSIFICATION_ENTRY:
            ptr = m_apis.bmtor_api->get_table_bitmap_classification_entry_stats;
            break;
        case SAI_OBJECT_TYPE_TABLE_BITMAP_ROUTER_ENTRY:
            ptr = m_apis.bmtor_api->get_table_bitmap_router_entry_stats;
            break;
        case SAI_OBJECT_TYPE_TABLE_META_TUNNEL_ENTRY:
            ptr = m_apis.bmtor_api->get_table_meta_tunnel_entry_stats;
            break;

        default:
            SWSS_LOG_ERROR("not implemented, FIXME");
            return SAI_STATUS_FAILURE;
    }

    return ptr(object_id, number_of_counters, counter_ids, counters);
}

sai_status_t VendorSai::getStatsExt(
        _In_ sai_object_type_t object_type,
        _In_ sai_object_id_t object_id,
        _In_ uint32_t number_of_counters,
        _In_ const sai_stat_id_t *counter_ids,
        _In_ sai_stats_mode_t mode,
        _Out_ uint64_t *counters)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    sai_status_t (*ptr)(
            _In_ sai_object_id_t port_id,
            _In_ uint32_t number_of_counters,
            _In_ const sai_stat_id_t *counter_ids,
            _In_ sai_stats_mode_t mode,
            _Out_ uint64_t *counters);

    switch ((int)object_type)
    {
        case SAI_OBJECT_TYPE_PORT:
            ptr = m_apis.port_api->get_port_stats_ext;
            break;
        case SAI_OBJECT_TYPE_ROUTER_INTERFACE:
            ptr = m_apis.router_interface_api->get_router_interface_stats_ext;
            break;
        case SAI_OBJECT_TYPE_POLICER:
            ptr = m_apis.policer_api->get_policer_stats_ext;
            break;
        case SAI_OBJECT_TYPE_QUEUE:
            ptr = m_apis.queue_api->get_queue_stats_ext;
            break;
        case SAI_OBJECT_TYPE_BUFFER_POOL:
            ptr = m_apis.buffer_api->get_buffer_pool_stats_ext;
            break;
        case SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP:
            ptr = m_apis.buffer_api->get_ingress_priority_group_stats_ext;
            break;
        case SAI_OBJECT_TYPE_SWITCH:
            ptr = m_apis.switch_api->get_switch_stats_ext;
            break;
        case SAI_OBJECT_TYPE_VLAN:
            ptr = m_apis.vlan_api->get_vlan_stats_ext;
            break;
        case SAI_OBJECT_TYPE_TUNNEL:
            ptr = m_apis.tunnel_api->get_tunnel_stats_ext;
            break;
        case SAI_OBJECT_TYPE_BRIDGE:
            ptr = m_apis.bridge_api->get_bridge_stats_ext;
            break;
        case SAI_OBJECT_TYPE_BRIDGE_PORT:
            ptr = m_apis.bridge_api->get_bridge_port_stats_ext;
            break;
        case SAI_OBJECT_TYPE_PORT_POOL:
            ptr = m_apis.port_api->get_port_pool_stats_ext;
            break;
        case SAI_OBJECT_TYPE_BFD_SESSION:
            ptr = m_apis.bfd_api->get_bfd_session_stats_ext;
            break;
        case SAI_OBJECT_TYPE_COUNTER:
            ptr = m_apis.counter_api->get_counter_stats_ext;
            break;
        case SAI_OBJECT_TYPE_TABLE_BITMAP_CLASSIFICATION_ENTRY:
            ptr = m_apis.bmtor_api->get_table_bitmap_classification_entry_stats_ext;
            break;
        case SAI_OBJECT_TYPE_TABLE_BITMAP_ROUTER_ENTRY:
            ptr = m_apis.bmtor_api->get_table_bitmap_router_entry_stats_ext;
            break;
        case SAI_OBJECT_TYPE_TABLE_META_TUNNEL_ENTRY:
            ptr = m_apis.bmtor_api->get_table_meta_tunnel_entry_stats_ext;
            break;

        default:
            SWSS_LOG_ERROR("not implemented, FIXME");
            return SAI_STATUS_FAILURE;
    }

    return ptr(object_id, number_of_counters, counter_ids, mode, counters);
}

sai_status_t VendorSai::clearStats(
        _In_ sai_object_type_t object_type,
        _In_ sai_object_id_t object_id,
        _In_ uint32_t number_of_counters,
        _In_ const sai_stat_id_t *counter_ids)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    sai_status_t (*ptr)(
            _In_ sai_object_id_t port_id,
            _In_ uint32_t number_of_counters,
            _In_ const sai_stat_id_t *counter_ids);

    switch ((int)object_type)
    {
        case SAI_OBJECT_TYPE_PORT:
            ptr = m_apis.port_api->clear_port_stats;
            break;
        case SAI_OBJECT_TYPE_ROUTER_INTERFACE:
            ptr = m_apis.router_interface_api->clear_router_interface_stats;
            break;
        case SAI_OBJECT_TYPE_POLICER:
            ptr = m_apis.policer_api->clear_policer_stats;
            break;
        case SAI_OBJECT_TYPE_QUEUE:
            ptr = m_apis.queue_api->clear_queue_stats;
            break;
        case SAI_OBJECT_TYPE_BUFFER_POOL:
            ptr = m_apis.buffer_api->clear_buffer_pool_stats;
            break;
        case SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP:
            ptr = m_apis.buffer_api->clear_ingress_priority_group_stats;
            break;
        case SAI_OBJECT_TYPE_SWITCH:
            ptr = m_apis.switch_api->clear_switch_stats;
            break;
        case SAI_OBJECT_TYPE_VLAN:
            ptr = m_apis.vlan_api->clear_vlan_stats;
            break;
        case SAI_OBJECT_TYPE_TUNNEL:
            ptr = m_apis.tunnel_api->clear_tunnel_stats;
            break;
        case SAI_OBJECT_TYPE_BRIDGE:
            ptr = m_apis.bridge_api->clear_bridge_stats;
            break;
        case SAI_OBJECT_TYPE_BRIDGE_PORT:
            ptr = m_apis.bridge_api->clear_bridge_port_stats;
            break;
        case SAI_OBJECT_TYPE_PORT_POOL:
            ptr = m_apis.port_api->clear_port_pool_stats;
            break;
        case SAI_OBJECT_TYPE_BFD_SESSION:
            ptr = m_apis.bfd_api->clear_bfd_session_stats;
            break;
        case SAI_OBJECT_TYPE_COUNTER:
            ptr = m_apis.counter_api->clear_counter_stats;
            break;
        case SAI_OBJECT_TYPE_TABLE_BITMAP_CLASSIFICATION_ENTRY:
            ptr = m_apis.bmtor_api->clear_table_bitmap_classification_entry_stats;
            break;
        case SAI_OBJECT_TYPE_TABLE_BITMAP_ROUTER_ENTRY:
            ptr = m_apis.bmtor_api->clear_table_bitmap_router_entry_stats;
            break;
        case SAI_OBJECT_TYPE_TABLE_META_TUNNEL_ENTRY:
            ptr = m_apis.bmtor_api->clear_table_meta_tunnel_entry_stats;
            break;

        default:
            SWSS_LOG_ERROR("not implemented, FIXME");
            return SAI_STATUS_FAILURE;
    }

    return ptr(object_id, number_of_counters, counter_ids);
}

// BULK QUAD OID

sai_status_t VendorSai::bulkCreate(
        _In_ sai_object_type_t object_type,
        _In_ sai_object_id_t switch_id,
        _In_ uint32_t object_count,
        _In_ const uint32_t *attr_count,
        _In_ const sai_attribute_t **attr_list,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_object_id_t *object_id,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    sai_status_t (*ptr)(
            _In_ sai_object_id_t switch_id,
            _In_ uint32_t object_count,
            _In_ const uint32_t *attr_count,
            _In_ const sai_attribute_t **attr_list,
            _In_ sai_bulk_op_error_mode_t mode,
            _Out_ sai_object_id_t *object_id,
            _Out_ sai_status_t *object_statuses);

    switch (object_type)
    {
        case SAI_OBJECT_TYPE_LAG_MEMBER:
            ptr = m_apis.lag_api->create_lag_members;
            break;

        case SAI_OBJECT_TYPE_NEXT_HOP_GROUP:
            ptr = m_apis.next_hop_group_api->create_next_hop_group_members;
            break;

        case SAI_OBJECT_TYPE_SEGMENTROUTE_SIDLIST:
            ptr = m_apis.segmentroute_api->create_segmentroute_sidlists;
            break;

        case SAI_OBJECT_TYPE_STP_PORT:
            ptr = m_apis.stp_api->create_stp_ports;
            break;

        case SAI_OBJECT_TYPE_VLAN_MEMBER:
            ptr = m_apis.vlan_api->create_vlan_members;
            break;

        default:
            SWSS_LOG_ERROR("not implemented, FIXME");
            return SAI_STATUS_NOT_IMPLEMENTED;
    }

    if (!ptr)
    {
        SWSS_LOG_INFO("create bulk not supported from SAI, object_type = %s",  sai_serialize_object_type(object_type).c_str());
        return SAI_STATUS_NOT_SUPPORTED;
    }

    return ptr(switch_id,
            object_count,
            attr_count,
            attr_list,
            mode,
            object_id,
            object_statuses);
}

sai_status_t VendorSai::bulkRemove(
        _In_ sai_object_type_t object_type,
        _In_ uint32_t object_count,
        _In_ const sai_object_id_t *object_id,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    sai_status_t (*ptr)(
            _In_ uint32_t object_count,
            _In_ const sai_object_id_t *object_id,
            _In_ sai_bulk_op_error_mode_t mode,
            _Out_ sai_status_t *object_statuses);

    switch (object_type)
    {
        case SAI_OBJECT_TYPE_LAG_MEMBER:
            ptr = m_apis.lag_api->remove_lag_members;
            break;

        case SAI_OBJECT_TYPE_NEXT_HOP_GROUP:
            ptr = m_apis.next_hop_group_api->remove_next_hop_group_members;
            break;

        case SAI_OBJECT_TYPE_SEGMENTROUTE_SIDLIST:
            ptr = m_apis.segmentroute_api->remove_segmentroute_sidlists;
            break;

        case SAI_OBJECT_TYPE_STP_PORT:
            ptr = m_apis.stp_api->remove_stp_ports;
            break;

        case SAI_OBJECT_TYPE_VLAN_MEMBER:
            ptr = m_apis.vlan_api->remove_vlan_members;
            break;

        default:
            SWSS_LOG_ERROR("not implemented, FIXME");
            return SAI_STATUS_NOT_IMPLEMENTED;
    }

    if (!ptr)
    {
        SWSS_LOG_INFO("remove bulk not supported from SAI, object_type = %s",  sai_serialize_object_type(object_type).c_str());
        return SAI_STATUS_NOT_SUPPORTED;
    }

    return ptr(object_count, object_id, mode, object_statuses);
}

sai_status_t VendorSai::bulkSet(
        _In_ sai_object_type_t object_type,
        _In_ uint32_t object_count,
        _In_ const sai_object_id_t *object_id,
        _In_ const sai_attribute_t *attr_list,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    SWSS_LOG_ERROR("not supported by SAI");

    return SAI_STATUS_NOT_SUPPORTED;
}

// BULK QUAD ENTRY

sai_status_t VendorSai::bulkCreate(
        _In_ uint32_t object_count,
        _In_ const sai_route_entry_t* entries,
        _In_ const uint32_t *attr_count,
        _In_ const sai_attribute_t **attr_list,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    if (!m_apis.route_api->create_route_entries)
    {
        SWSS_LOG_INFO("create_route_entries is not supported");
        return SAI_STATUS_NOT_SUPPORTED;
    }

    return m_apis.route_api->create_route_entries(
            object_count,
            entries,
            attr_count,
            attr_list,
            mode,
            object_statuses);
}

sai_status_t VendorSai::bulkCreate(
        _In_ uint32_t object_count,
        _In_ const sai_fdb_entry_t* entries,
        _In_ const uint32_t *attr_count,
        _In_ const sai_attribute_t **attr_list,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    if (!m_apis.fdb_api->create_fdb_entries)
    {
        SWSS_LOG_INFO("create_fdb_entries is not supported");
        return SAI_STATUS_NOT_SUPPORTED;
    }

    return m_apis.fdb_api->create_fdb_entries(
            object_count,
            entries,
            attr_count,
            attr_list,
            mode,
            object_statuses);
}

sai_status_t VendorSai::bulkCreate(
        _In_ uint32_t object_count,
        _In_ const sai_nat_entry_t* entries,
        _In_ const uint32_t *attr_count,
        _In_ const sai_attribute_t **attr_list,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    if (!m_apis.nat_api->create_nat_entries)
    {
        SWSS_LOG_INFO("create_nat_entries is not supported");
        return SAI_STATUS_NOT_SUPPORTED;
    }

    return m_apis.nat_api->create_nat_entries(
            object_count,
            entries,
            attr_count,
            attr_list,
            mode,
            object_statuses);
}

// BULK REMOVE

sai_status_t VendorSai::bulkRemove(
        _In_ uint32_t object_count,
        _In_ const sai_route_entry_t *entries,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    if (!m_apis.route_api->remove_route_entries)
    {
        SWSS_LOG_INFO("remove_route_entries is not supported");
        return SAI_STATUS_NOT_SUPPORTED;
    }

    return m_apis.route_api->remove_route_entries(
            object_count,
            entries,
            mode,
            object_statuses);
}


sai_status_t VendorSai::bulkRemove(
        _In_ uint32_t object_count,
        _In_ const sai_fdb_entry_t *entries,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    if (!m_apis.fdb_api->remove_fdb_entries)
    {
        SWSS_LOG_INFO("remove_fdb_entries is not supported");
        return SAI_STATUS_NOT_SUPPORTED;
    }

    return m_apis.fdb_api->remove_fdb_entries(
            object_count,
            entries,
            mode,
            object_statuses);;
}

sai_status_t VendorSai::bulkRemove(
        _In_ uint32_t object_count,
        _In_ const sai_nat_entry_t *entries,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    if (!m_apis.nat_api->remove_nat_entries)
    {
        SWSS_LOG_INFO("remove_nat_entries is not supported");
        return SAI_STATUS_NOT_SUPPORTED;
    }

    return m_apis.nat_api->remove_nat_entries(
            object_count,
            entries,
            mode,
            object_statuses);
}

// BULK SET

sai_status_t VendorSai::bulkSet(
        _In_ uint32_t object_count,
        _In_ const sai_route_entry_t *entries,
        _In_ const sai_attribute_t *attr_list,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    if (!m_apis.route_api->set_route_entries_attribute)
    {
        SWSS_LOG_INFO("set_route_entries_attribute is not supported");
        return SAI_STATUS_NOT_SUPPORTED;
    }

    return m_apis.route_api->set_route_entries_attribute(
            object_count,
            entries,
            attr_list,
            mode,
            object_statuses);
}

sai_status_t VendorSai::bulkSet(
        _In_ uint32_t object_count,
        _In_ const sai_fdb_entry_t *entries,
        _In_ const sai_attribute_t *attr_list,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    if (!m_apis.fdb_api->set_fdb_entries_attribute)
    {
        SWSS_LOG_INFO("set_fdb_entries_attribute is not supported");
        return SAI_STATUS_NOT_SUPPORTED;
    }

    return m_apis.fdb_api->set_fdb_entries_attribute(
            object_count,
            entries,
            attr_list,
            mode,
            object_statuses);;
}

sai_status_t VendorSai::bulkSet(
        _In_ uint32_t object_count,
        _In_ const sai_nat_entry_t *entries,
        _In_ const sai_attribute_t *attr_list,
        _In_ sai_bulk_op_error_mode_t mode,
        _Out_ sai_status_t *object_statuses)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    if (!m_apis.nat_api->set_nat_entries_attribute)
    {
        SWSS_LOG_INFO("set_nat_entries_attribute is not supported");
        return SAI_STATUS_NOT_SUPPORTED;
    }

    return m_apis.nat_api->set_nat_entries_attribute(
            object_count,
            entries,
            attr_list,
            mode,
            object_statuses);
}

// NON QUAD API

sai_status_t VendorSai::flushFdbEntries(
        _In_ sai_object_id_t switch_id,
        _In_ uint32_t attr_count,
        _In_ const sai_attribute_t *attr_list)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    return m_apis.fdb_api->flush_fdb_entries(switch_id, attr_count, attr_list);
}

// SAI API

sai_status_t VendorSai::objectTypeGetAvailability(
        _In_ sai_object_id_t switchId,
        _In_ sai_object_type_t objectType,
        _In_ uint32_t attrCount,
        _In_ const sai_attribute_t *attrList,
        _Out_ uint64_t *count)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    return sai_object_type_get_availability(
            switchId,
            objectType,
            attrCount,
            attrList,
            count);
}

sai_status_t VendorSai::queryAttributeCapability(
        _In_ sai_object_id_t switchId,
        _In_ sai_object_type_t objectType,
        _In_ sai_attr_id_t attrId,
        _Out_ sai_attr_capability_t *capability)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    return sai_query_attribute_capability(
            switchId,
            objectType,
            attrId,
            capability);
}

sai_status_t VendorSai::queryAattributeEnumValuesCapability(
        _In_ sai_object_id_t switchId,
        _In_ sai_object_type_t objectType,
        _In_ sai_attr_id_t attrId,
        _Inout_ sai_s32_list_t *enum_values_capability)
{
    MUTEX();
    SWSS_LOG_ENTER();
    VENDOR_CHECK_API_INITIALIZED();

    return sai_query_attribute_enum_values_capability(
            switchId,
            objectType,
            attrId,
            enum_values_capability);
}

sai_object_type_t VendorSai::objectTypeQuery(
        _In_ sai_object_id_t objectId)
{
    SWSS_LOG_ENTER();

    if (!m_apiInitialized)
    {
        SWSS_LOG_ERROR("%s: SAI API not initialized", __PRETTY_FUNCTION__);

        return SAI_OBJECT_TYPE_NULL;
    }

    return sai_object_type_query(objectId);
}

sai_object_id_t VendorSai::switchIdQuery(
        _In_ sai_object_id_t objectId)
{
    SWSS_LOG_ENTER();

    if (!m_apiInitialized)
    {
        SWSS_LOG_ERROR("%s: SAI API not initialized", __PRETTY_FUNCTION__);

        return SAI_NULL_OBJECT_ID;
    }

    return sai_switch_id_query(objectId);
}

sai_status_t VendorSai::logSet(
        _In_ sai_api_t api,
        _In_ sai_log_level_t log_level)
{
    SWSS_LOG_ENTER();

    return sai_log_set(api, log_level);
}
