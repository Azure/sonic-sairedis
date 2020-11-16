#include "MetaKeyHasher.h"
#include "sai_serialize.h"

#include "swss/logger.h"

#include <cstring>

using namespace saimeta;

static bool operator==(
        const sai_fdb_entry_t& a,
        const sai_fdb_entry_t& b)
{
    SWSS_LOG_ENTER();

    return a.switch_id == b.switch_id &&
        a.bv_id == b.bv_id &&
        memcmp(a.mac_address, b.mac_address, sizeof(a.mac_address)) == 0;
}

static bool operator==(
        const sai_route_entry_t& a,
        const sai_route_entry_t& b)
{
    // SWSS_LOG_ENTER(); // disabled for performance reasons

    bool part = a.switch_id == b.switch_id &&
        a.vr_id == b.vr_id &&
        a.destination.addr_family == b.destination.addr_family;

    if (a.destination.addr_family == SAI_IP_ADDR_FAMILY_IPV4)
    {
        return part &&
            a.destination.addr.ip4 == b.destination.addr.ip4 &&
            a.destination.mask.ip4 == b.destination.mask.ip4;
    }

    if (a.destination.addr_family == SAI_IP_ADDR_FAMILY_IPV6)
    {
        return part &&
            memcmp(a.destination.addr.ip6, b.destination.addr.ip6, sizeof(b.destination.addr.ip6)) == 0 &&
            memcmp(a.destination.mask.ip6, b.destination.mask.ip6, sizeof(b.destination.mask.ip6)) == 0;
    }

    return part;
}

static bool operator==(
        const sai_neighbor_entry_t& a,
        const sai_neighbor_entry_t& b)
{
    SWSS_LOG_ENTER();

    bool part = a.switch_id == b.switch_id &&
        a.rif_id == b.rif_id &&
        a.ip_address.addr_family == b.ip_address.addr_family;

    if (a.ip_address.addr_family == SAI_IP_ADDR_FAMILY_IPV4)
        return part && a.ip_address.addr.ip4 == b.ip_address.addr.ip4;

    if (a.ip_address.addr_family == SAI_IP_ADDR_FAMILY_IPV6)
        return part && memcmp(a.ip_address.addr.ip6, b.ip_address.addr.ip6, sizeof(b.ip_address.addr.ip6)) == 0;

    return part;
}

bool MetaKeyHasher::operator()(
        const sai_object_meta_key_t& a,
        const sai_object_meta_key_t& b) const
{
    // SWSS_LOG_ENTER(); // disabled for performance reasons

    if (a.objecttype != b.objecttype)
        return false;

    auto meta = sai_metadata_get_object_type_info(a.objecttype);

    if (meta && meta->isobjectid)
        return a.objectkey.key.object_id == b.objectkey.key.object_id;

    if (a.objecttype == SAI_OBJECT_TYPE_ROUTE_ENTRY)
        return a.objectkey.key.route_entry == b.objectkey.key.route_entry;

    if (a.objecttype == SAI_OBJECT_TYPE_NEIGHBOR_ENTRY)
        return a.objectkey.key.neighbor_entry == b.objectkey.key.neighbor_entry;

    if (a.objecttype == SAI_OBJECT_TYPE_FDB_ENTRY)
        return a.objectkey.key.fdb_entry == b.objectkey.key.fdb_entry;

    SWSS_LOG_THROW("not implemented: %s",
            sai_serialize_object_meta_key(a).c_str());
}

static_assert(sizeof(std::size_t) >= sizeof(uint32_t), "size_t must be at least 32 bits");

static inline std::size_t sai_get_hash(
        _In_ const sai_route_entry_t& re)
{
    // SWSS_LOG_ENTER(); // disabled for performance reasons

    if (re.destination.addr_family == SAI_IP_ADDR_FAMILY_IPV4)
    {
        return re.destination.addr.ip4;
    }

    if (re.destination.addr_family == SAI_IP_ADDR_FAMILY_IPV6)
    {
        const uint32_t* ip6 = (const uint32_t*)re.destination.addr.ip6;

        return ip6[0] ^ ip6[1] ^ ip6[2] ^ ip6[3];
    }

    return re.destination.addr_family;
}

static inline std::size_t sai_get_hash(
        _In_ const sai_neighbor_entry_t& ne)
{
    SWSS_LOG_ENTER();

    if (ne.ip_address.addr_family == SAI_IP_ADDR_FAMILY_IPV4)
    {
        return ne.ip_address.addr.ip4;
    }

    if (ne.ip_address.addr_family == SAI_IP_ADDR_FAMILY_IPV6)
    {
        const uint32_t* ip6 = (const uint32_t*)ne.ip_address.addr.ip6;

        return ip6[0] ^ ip6[1] ^ ip6[2] ^ ip6[3];
    }

    return ne.ip_address.addr_family;
}

static inline std::size_t sai_get_hash(
        _In_ const sai_fdb_entry_t& fe)
{
    SWSS_LOG_ENTER();

    return *(const uint32_t*)(&fe.mac_address[2]);
}

static inline std::size_t sai_get_hash(
        _In_ const sai_nat_entry_t& ne)
{
    SWSS_LOG_ENTER();

    // TODO revisit - may depend on nat_type

    return ne.data.key.src_ip ^ ne.data.key.dst_ip;
}

std::size_t MetaKeyHasher::operator()(
        _In_ const sai_object_meta_key_t& k) const
{
    // SWSS_LOG_ENTER(); // disabled for performance reasons

    auto meta = sai_metadata_get_object_type_info(k.objecttype);

    if (meta && meta->isobjectid)
    {
        return k.objectkey.key.object_id;
    }

    switch (k.objecttype)
    {
        case SAI_OBJECT_TYPE_ROUTE_ENTRY:
            return sai_get_hash(k.objectkey.key.route_entry);

        case SAI_OBJECT_TYPE_NEIGHBOR_ENTRY:
            return sai_get_hash(k.objectkey.key.neighbor_entry);

        case SAI_OBJECT_TYPE_FDB_ENTRY:
            return sai_get_hash(k.objectkey.key.fdb_entry);

        case SAI_OBJECT_TYPE_NAT_ENTRY:
            return sai_get_hash(k.objectkey.key.nat_entry);

        default:
            SWSS_LOG_THROW("not handled: %s", sai_serialize_object_type(k.objecttype).c_str());
    }
}
