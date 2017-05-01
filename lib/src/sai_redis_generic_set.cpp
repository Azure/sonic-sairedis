#include "sai_redis.h"
#include "meta/saiserialize.h"
#include "meta/saiattributelist.h"

sai_status_t internal_redis_generic_set(
        _In_ sai_object_type_t object_type,
        _In_ const std::string &serialized_object_id,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    std::vector<swss::FieldValueTuple> entry = SaiAttributeList::serialize_attr_list(
            object_type,
            1,
            attr,
            false);

    std::string str_object_type = sai_serialize_object_type(object_type);

    std::string key = str_object_type + ":" + serialized_object_id;

    SWSS_LOG_DEBUG("generic set key: %s, fields: %lu", key.c_str(), entry.size());

    if (g_record)
    {
        recordLine("s|" + key + "|" + joinFieldValues(entry));
    }

    g_asicState->set(key, entry, "set");

    return SAI_STATUS_SUCCESS;
}

sai_status_t internal_redis_bulk_generic_set(
        _In_ sai_object_type_t object_type,
        _In_ const std::vector<std::string> &serialized_object_ids,
        _In_ const sai_attribute_t *attr_list,
        _In_ const sai_status_t *object_statuses)
{
    SWSS_LOG_ENTER();

    std::string str_object_type = sai_serialize_object_type(object_type);

    std::vector<swss::FieldValueTuple> entries;

    for (size_t idx = 0; idx < serialized_object_ids.size(); ++idx)
    {
        std::vector<swss::FieldValueTuple> entry =
            SaiAttributeList::serialize_attr_list(object_type, 1, &attr_list[idx], false);

        std::string str_attr = joinFieldValues(entry);

        std::string str_status = sai_serialize_status(object_statuses[idx]);

        std::string joined = str_attr + "|" + str_status;

        swss::FieldValueTuple fvt(serialized_object_ids[idx] , joined);

        entries.push_back(fvt);
    }

    std::string key = str_object_type;

    if (g_record)
    {
        std::string joined = "";

        for (const auto &e: entries)
        {
            // ||obj_id|attr=val|attr=val|status||obj_id|attr=val|attr=val|status

            joined += "||" + fvField(e) + "|" + fvValue(e);
        }

        /*
         * Capital 'S' stads for bulk SET operation.
         */

        recordLine("S|" + key + joined);
    }

    g_asicState->set(key, entries, "bulkset");

    return SAI_STATUS_SUCCESS;
}


sai_status_t redis_generic_set(
        _In_ sai_object_type_t object_type,
        _In_ sai_object_id_t object_id,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    if (object_id == SAI_NULL_OBJECT_ID && object_type != SAI_OBJECT_TYPE_SWITCH)
    {
        SWSS_LOG_ERROR("object id is zero on object type %d", object_type);

        return SAI_STATUS_INVALID_PARAMETER;
    }

    std::string str_object_id = sai_serialize_object_id(object_id);

    return internal_redis_generic_set(
            object_type,
            str_object_id,
            attr);
}

sai_status_t redis_generic_set_fdb_entry(
        _In_ const sai_fdb_entry_t *fdb_entry,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    std::string str_fdb_entry = sai_serialize_fdb_entry(*fdb_entry);

    return internal_redis_generic_set(
            SAI_OBJECT_TYPE_FDB,
            str_fdb_entry,
            attr);
}

sai_status_t redis_generic_set_neighbor_entry(
        _In_ const sai_neighbor_entry_t* neighbor_entry,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    std::string str_neighbor_entry = sai_serialize_neighbor_entry(*neighbor_entry);

    return internal_redis_generic_set(
            SAI_OBJECT_TYPE_NEIGHBOR,
            str_neighbor_entry,
            attr);
}

sai_status_t redis_generic_set_route_entry(
        _In_ const sai_unicast_route_entry_t* unicast_route_entry,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    std::string str_route_entry = sai_serialize_route_entry(*unicast_route_entry);

    return internal_redis_generic_set(
            SAI_OBJECT_TYPE_ROUTE,
            str_route_entry,
            attr);
}

sai_status_t redis_generic_set_vlan(
        _In_ sai_vlan_id_t vlan_id,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    std::string str_vlan_id = sai_serialize_vlan_id(vlan_id);

    return internal_redis_generic_set(
            SAI_OBJECT_TYPE_VLAN,
            str_vlan_id,
            attr);
}

sai_status_t redis_generic_set_trap(
        _In_ sai_hostif_trap_id_t hostif_trap_id,
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    std::string str_hostif_trap_id = sai_serialize_hostif_trap_id(hostif_trap_id);

    return internal_redis_generic_set(
            SAI_OBJECT_TYPE_TRAP,
            str_hostif_trap_id,
            attr);
}

sai_status_t redis_generic_set_switch(
        _In_ const sai_attribute_t *attr)
{
    SWSS_LOG_ENTER();

    sai_object_id_t object_id = 0;

    std::string str_object_id = sai_serialize_object_id(object_id);

    return internal_redis_generic_set(
            SAI_OBJECT_TYPE_SWITCH,
            str_object_id,
            attr);
}
