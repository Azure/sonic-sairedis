#include "VirtualOidTranslator.h"
#include "VirtualObjectIdManager.h"
#include "RedisClient.h"

#include "swss/logger.h"
#include "meta/sai_serialize.h"

#include <inttypes.h>

using namespace syncd;

VirtualOidTranslator::VirtualOidTranslator(
        _In_ std::shared_ptr<RedisClient> client,
        _In_ std::shared_ptr<sairedis::VirtualObjectIdManager> virtualObjectIdManager,
        _In_ std::shared_ptr<sairedis::SaiInterface> vendorSai):
    m_virtualObjectIdManager(virtualObjectIdManager),
    m_vendorSai(vendorSai),
    m_client(client)
{
    SWSS_LOG_ENTER();

    // empty
}

bool VirtualOidTranslator::tryTranslateRidToVid(
        _In_ sai_object_id_t rid,
        _Out_ sai_object_id_t &vid)
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    if (rid == SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_DEBUG("translated RID null to VID null");

        vid = SAI_NULL_OBJECT_ID;
        return true;
    }

    auto it = m_rid2vid.find(vid);

    if (it != m_rid2vid.end())
    {
        vid = it->second;
        return true;
    }

    vid = m_client->getVidForRid(rid);

    if (vid == SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_DEBUG("translated RID %s to VID null", sai_serialize_object_id(rid).c_str());
        return false;
    }

    return true;
}

sai_object_id_t VirtualOidTranslator::translateRidToVid(
        _In_ sai_object_id_t rid,
        _In_ sai_object_id_t switchVid,
        _In_ bool translateRemoved)
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    /*
     * NOTE: switch_vid here is Virtual ID of switch for which we need
     * create VID for given RID.
     */

    if (rid == SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_DEBUG("translated RID null to VID null");

        return SAI_NULL_OBJECT_ID;
    }

    auto it = m_rid2vid.find(rid);

    if (it != m_rid2vid.end())
    {
        return it->second;
    }

    std::string strRid = sai_serialize_object_id(rid);

    auto vid = m_client->getVidForRid(rid);

    if (vid != SAI_NULL_OBJECT_ID)
    {
        // object exists

        SWSS_LOG_DEBUG("translated RID %s to VID %s",
                sai_serialize_object_id(rid).c_str(),
                sai_serialize_object_id(vid).c_str());

        return vid;
    }

    if (translateRemoved)
    {
        auto itr = m_removedRid2vid.find(rid);

        if (itr !=  m_removedRid2vid.end())
        {
            SWSS_LOG_WARN("translating removed RID %s, to VID %s",
                    sai_serialize_object_id(rid).c_str(),
                    sai_serialize_object_id(itr->second).c_str());

            return itr->second;
        }
    }

    SWSS_LOG_DEBUG("spotted new RID %s", sai_serialize_object_id(rid).c_str());

    sai_object_type_t object_type = m_vendorSai->objectTypeQuery(rid); // TODO move to std::function or wrapper class

    if (object_type == SAI_OBJECT_TYPE_NULL)
    {
        SWSS_LOG_THROW("vendorSai->objectTypeQuery returned NULL type for RID 0x%" PRIx64, rid);
    }

    if (object_type == SAI_OBJECT_TYPE_SWITCH)
    {
        /*
         * Switch ID should be already inside local db or redis db when we
         * created switch, so we should never get here.
         */

        SWSS_LOG_THROW("RID 0x%" PRIx64 " is switch object, but not in local or redis db, bug!", rid);
    }

    vid = m_virtualObjectIdManager->allocateNewObjectId(object_type, switchVid); // TODO to std::function or separate object

    SWSS_LOG_INFO("translated RID %s to VID %s",
            sai_serialize_object_id(rid).c_str(),
            sai_serialize_object_id(vid).c_str());

    m_client->insertVidAndRid(vid, rid);

    m_rid2vid[rid] = vid;
    m_vid2rid[vid] = rid;

    return vid;
}

bool VirtualOidTranslator::checkRidExists(
        _In_ sai_object_id_t rid,
        _In_ bool checkRemoved)
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    if (rid == SAI_NULL_OBJECT_ID)
        return true;

    if (m_rid2vid.find(rid) != m_rid2vid.end())
        return true;

    auto vid = m_client->getVidForRid(rid);

    if (vid != SAI_NULL_OBJECT_ID)
        return true;

    if (checkRemoved && (m_removedRid2vid.find(rid) != m_removedRid2vid.end()))
    {
        SWSS_LOG_WARN("removed RID %s exists", sai_serialize_object_id(rid).c_str());
        return true;
    }

    return false;
}

void VirtualOidTranslator::translateRidToVid(
        _Inout_ sai_object_list_t &element,
        _In_ sai_object_id_t switchVid,
        _In_ bool translateRemoved)
{
    SWSS_LOG_ENTER();

    for (uint32_t i = 0; i < element.count; i++)
    {
        element.list[i] = translateRidToVid(element.list[i], switchVid, translateRemoved);
    }
}

void VirtualOidTranslator::translateRidToVid(
        _In_ sai_object_type_t objectType,
        _In_ sai_object_id_t switchVid,
        _In_ uint32_t attr_count,
        _Inout_ sai_attribute_t *attrList,
        _In_ bool translateRemoved)
{
    SWSS_LOG_ENTER();

    /*
     * We receive real id's here, if they are new then create new VIDs for them
     * and put in db, if entry exists in db, use it.
     *
     * NOTE: switch_id is VID of switch on which those RIDs are provided.
     */

    for (uint32_t i = 0; i < attr_count; i++)
    {
        sai_attribute_t &attr = attrList[i];

        auto meta = sai_metadata_get_attr_metadata(objectType, attr.id);

        if (meta == NULL)
        {
            SWSS_LOG_THROW("unable to get metadata for object type %x, attribute %d", objectType, attr.id);
        }

        /*
         * TODO: Many times we do switch for list of attributes to perform some
         * operation on each oid from that attribute, we should provide clever
         * way via sai metadata utils to get that.
         */

        switch (meta->attrvaluetype)
        {
            case SAI_ATTR_VALUE_TYPE_OBJECT_ID:
                attr.value.oid = translateRidToVid(attr.value.oid, switchVid, translateRemoved);
                break;

            case SAI_ATTR_VALUE_TYPE_OBJECT_LIST:
                translateRidToVid(attr.value.objlist, switchVid, translateRemoved);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_FIELD_DATA_OBJECT_ID:
                if (attr.value.aclfield.enable)
                    attr.value.aclfield.data.oid = translateRidToVid(attr.value.aclfield.data.oid, switchVid, translateRemoved);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_FIELD_DATA_OBJECT_LIST:
                if (attr.value.aclfield.enable)
                    translateRidToVid(attr.value.aclfield.data.objlist, switchVid, translateRemoved);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_ACTION_DATA_OBJECT_ID:
                if (attr.value.aclaction.enable)
                    attr.value.aclaction.parameter.oid = translateRidToVid(attr.value.aclaction.parameter.oid, switchVid, translateRemoved);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_ACTION_DATA_OBJECT_LIST:
                if (attr.value.aclaction.enable)
                    translateRidToVid(attr.value.aclaction.parameter.objlist, switchVid, translateRemoved);
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
    }
}

sai_object_id_t VirtualOidTranslator::translateVidToRid(
        _In_ sai_object_id_t vid)
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    if (vid == SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_DEBUG("translated VID null to RID null");

        return SAI_NULL_OBJECT_ID;
    }

    auto it = m_vid2rid.find(vid);

    if (it != m_vid2rid.end())
    {
        return it->second;
    }

    auto rid = m_client->getRidForVid(vid);

    if (rid == SAI_NULL_OBJECT_ID)
    {
            /*
             * If user created object that is object id, then it should not
             * query attributes of this object in init view mode, because he
             * knows all attributes passed to that object.
             *
             * NOTE: This may be a problem for some objects in init view mode.
             * We will need to revisit this after checking with real SAI
             * implementation.  Problem here may be that user will create some
             * object and actually will need to to query some of it's values,
             * like buffer limitations etc, mostly probably this will happen on
             * SWITCH object.
             */

            // SWSS_LOG_THROW("can't get RID in init view mode - don't query created objects");

        SWSS_LOG_THROW("unable to get RID for VID %s",
                sai_serialize_object_id(vid).c_str());
    }

    /*
     * We got this RID from redis db, so put it also to local db so it will be
     * faster to retrieve it late on.
     */

    m_vid2rid[vid] = rid;

    SWSS_LOG_DEBUG("translated VID %s to RID %s",
            sai_serialize_object_id(vid).c_str(),
            sai_serialize_object_id(rid).c_str());

    return rid;
}

/*
 * NOTE: We could have in metadata utils option to execute function on each
 * object on oid like this.  Problem is that we can't then add extra
 * parameters.
 */

bool VirtualOidTranslator::tryTranslateVidToRid(
        _In_ sai_object_id_t vid,
        _Out_ sai_object_id_t& rid)
{
    SWSS_LOG_ENTER();

    try
    {
        rid = translateVidToRid(vid);
        return true;
    }
    catch (const std::exception& e)
    {
        // message was logged already when throwing
        return false;
    }
}

bool VirtualOidTranslator::tryTranslateVidToRid(
        _Inout_ sai_object_meta_key_t &metaKey)
{
    SWSS_LOG_ENTER();

    try
    {
        translateVidToRid(metaKey);
        return true;
    }
    catch (const std::exception& e)
    {
        // message was logged already when throwing
        return false;
    }
}

void VirtualOidTranslator::translateVidToRid(
        _Inout_ sai_object_list_t &element)
{
    SWSS_LOG_ENTER();

    for (uint32_t i = 0; i < element.count; i++)
    {
        element.list[i] = translateVidToRid(element.list[i]);
    }
}

void VirtualOidTranslator::translateVidToRid(
        _In_ sai_object_type_t objectType,
        _In_ uint32_t attr_count,
        _Inout_ sai_attribute_t *attrList)
{
    SWSS_LOG_ENTER();

    /*
     * All id's received from sairedis should be virtual, so lets translate
     * them to real id's before we execute actual api.
     */

    for (uint32_t i = 0; i < attr_count; i++)
    {
        sai_attribute_t &attr = attrList[i];

        auto meta = sai_metadata_get_attr_metadata(objectType, attr.id);

        if (meta == NULL)
        {
            SWSS_LOG_THROW("unable to get metadata for object type %x, attribute %d", objectType, attr.id);
        }

        switch (meta->attrvaluetype)
        {
            case SAI_ATTR_VALUE_TYPE_OBJECT_ID:
                attr.value.oid = translateVidToRid(attr.value.oid);
                break;

            case SAI_ATTR_VALUE_TYPE_OBJECT_LIST:
                translateVidToRid(attr.value.objlist);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_FIELD_DATA_OBJECT_ID:
                if (attr.value.aclfield.enable)
                    attr.value.aclfield.data.oid = translateVidToRid(attr.value.aclfield.data.oid);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_FIELD_DATA_OBJECT_LIST:
                if (attr.value.aclfield.enable)
                    translateVidToRid(attr.value.aclfield.data.objlist);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_ACTION_DATA_OBJECT_ID:
                if (attr.value.aclaction.enable)
                    attr.value.aclaction.parameter.oid = translateVidToRid(attr.value.aclaction.parameter.oid);
                break;

            case SAI_ATTR_VALUE_TYPE_ACL_ACTION_DATA_OBJECT_LIST:
                if (attr.value.aclaction.enable)
                    translateVidToRid(attr.value.aclaction.parameter.objlist);
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
    }
}

void VirtualOidTranslator::translateVidToRid(
        _Inout_ sai_object_meta_key_t &metaKey)
{
    SWSS_LOG_ENTER();

    auto info = sai_metadata_get_object_type_info(metaKey.objecttype);

    if (info->isobjectid)
    {
        metaKey.objectkey.key.object_id =
            translateVidToRid(metaKey.objectkey.key.object_id);

        return;
    }

    for (size_t j = 0; j < info->structmemberscount; ++j)
    {
        const sai_struct_member_info_t *m = info->structmembers[j];

        if (m->membervaluetype == SAI_ATTR_VALUE_TYPE_OBJECT_ID)
        {
            sai_object_id_t vid = m->getoid(&metaKey);

            sai_object_id_t rid = translateVidToRid(vid);

            m->setoid(&metaKey, rid);
        }
    }
}

void VirtualOidTranslator::insertRidAndVid(
        _In_ sai_object_id_t rid,
        _In_ sai_object_id_t vid)
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    // to support multiple switches vid/rid map must be per switch

    m_rid2vid[rid] = vid;
    m_vid2rid[vid] = rid;

    m_client->insertVidAndRid(vid, rid);
}

void VirtualOidTranslator::eraseRidAndVid(
        _In_ sai_object_id_t rid,
        _In_ sai_object_id_t vid)
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    m_client->removeVidAndRid(vid, rid);

    // remove from local vid2rid and rid2vid map

    m_rid2vid.erase(rid);
    m_vid2rid.erase(vid);

    m_removedRid2vid[rid] = vid;
}

void VirtualOidTranslator::clearLocalCache()
{
    SWSS_LOG_ENTER();

    std::lock_guard<std::mutex> lock(m_mutex);

    m_rid2vid.clear();
    m_vid2rid.clear();

    m_removedRid2vid.clear();
}
