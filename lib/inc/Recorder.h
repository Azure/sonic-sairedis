#pragma once

extern "C" {
#include "sai.h"
}

#include "swss/table.h"

#include "sairedis.h" // for notify enum

#include <string>
#include <fstream>
#include <vector>

#define SAI_REDIS_RECORDER_DECLARE_RECORD_REMOVE(ot)    \
    void recordRemove(                                  \
            _In_ const sai_ ## ot ## _t* ot);

#define SAI_REDIS_RECORDER_DECLARE_RECORD_CREATE(ot)    \
    void recordCreate(                                  \
            _In_ const sai_ ## ot ## _t* ot,            \
            _In_ uint32_t attr_count,                   \
            _In_ const sai_attribute_t *attr_list);

#define SAI_REDIS_RECORDER_DECLARE_RECORD_SET(ot)       \
    void recordSet(                                     \
            _In_ const sai_ ## ot ## _t* ot,            \
            _In_ const sai_attribute_t *attr);

#define SAI_REDIS_RECORDER_DECLARE_RECORD_GET(ot)       \
    void recordGet(                                     \
            _In_ const sai_ ## ot ## _t* ot,            \
            _In_ uint32_t attr_count,                   \
            _In_ const sai_attribute_t *attr_list);

namespace sairedis
{
    class Recorder
    {
        public:

            Recorder();

            virtual ~Recorder();

        public: // SAI API

            // Ideally we would have each record api here with exact the same
            // attributes as SAI apis, but since we use serialize methods for
            // attributes and the same format is used in recorder and in REDIS
            // database in message queue, it make sense to reuse those
            // serialized values to not double serialize the same attributes.

            // generic create, remove, set, get
            // generic bulk apis
            //
            // sai_flush_fdb_entries
            // sai_remove_all_neighbor_entries
            // sai_clear_port_all_stats
            //
            // sai_api_query
            // sai_log_set
            // sai_api_initialize
            // sai_api_uninitialize
            // sai_dbg_generate_dump
            // sai_object_type_get_availability
            // sai_get_maximum_attribute_count
            // sai_get_object_key
            // sai_get_object_count
            // sai_query_attribute_capability
            // sai_query_attribute_enum_values_capability
            // sai_bulk_get_attribute
            // sai_tam_telemetry_get_data

        public: // SAI quad API

            /**
             * @brief Record generic Create API.
             *
             * NOTE: recordCreate should log (object_type, switch_id,
             * attr_count, attr_list) which we want to create new object id.
             * We omit object id since we assume that object id was already
             * allocated/generated by syncd, but this approach will not work in
             * asynchronous mode, so we are allocating new object id before
             * calling SAI create API by using VirtualObjectIdManager.  That's
             * why record interface here is different than SAI interface.
             *
             * Also currently in sai player response for create is not expected.
             */
            void recordGenericCreate(
                    _In_ sai_object_type_t objectType,
                    _In_ sai_object_id_t objectId, // already allocated
                    _In_ uint32_t attr_count,
                    _In_ const sai_attribute_t *attr_list);

            // TODO to be private
            void recordGenericCreate(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            /**
             * @brief Record generic Create API response.
             *
             * @param status Returned status from SAI implementation.
             * @param objectId Newly allocated object ID if call to create succeeded.
             */
            void recordGenericCreateResponse(
                    _In_ sai_status_t status,
                    _In_ sai_object_id_t objectId);

            void recordGenericCreateResponse(
                    _In_ sai_status_t status);

            void recordGenericRemove(
                    _In_ sai_object_type_t objectType,
                    _In_ sai_object_id_t objectId);

            // TODO to be private
            void recordGenericRemove(
                    _In_ const std::string& key);

            void recordGenericRemoveResponse(
                    _In_ sai_status_t status);

            void recordGenericSet(
                    _In_ sai_object_type_t objectType,
                    _In_ sai_object_id_t objectId,
                    _In_ const sai_attribute_t *attr);

            // TODO to be private
            void recordGenericSet(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordGenericSetResponse(
                    _In_ sai_status_t status);

            void recordGenericGet(
                    _In_ sai_object_type_t objectType,
                    _In_ sai_object_id_t objectId,
                    _In_ uint32_t attr_count,
                    _In_ const sai_attribute_t *attr_list);

            // TODO to be private
            void recordGenericGet(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordGenericGetResponse(
                    _In_ sai_status_t status,
                    _In_ sai_object_type_t objectType,
                    _In_ uint32_t attr_count,
                    _In_ const sai_attribute_t *attr_list);

            // TODO to be private
            void recordGenericGetResponse(
                    _In_ sai_status_t status,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

        public: // create ENTRY

            SAI_REDIS_RECORDER_DECLARE_RECORD_CREATE(fdb_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_CREATE(inseg_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_CREATE(ipmc_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_CREATE(l2mc_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_CREATE(mcast_fdb_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_CREATE(neighbor_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_CREATE(route_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_CREATE(nat_entry);

        public: // remove ENTRY

            SAI_REDIS_RECORDER_DECLARE_RECORD_REMOVE(fdb_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_REMOVE(inseg_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_REMOVE(ipmc_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_REMOVE(l2mc_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_REMOVE(mcast_fdb_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_REMOVE(neighbor_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_REMOVE(route_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_REMOVE(nat_entry);

        public: // set ENTRY

            SAI_REDIS_RECORDER_DECLARE_RECORD_SET(fdb_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_SET(inseg_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_SET(ipmc_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_SET(l2mc_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_SET(mcast_fdb_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_SET(neighbor_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_SET(route_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_SET(nat_entry);

        public: // get ENTRY

            SAI_REDIS_RECORDER_DECLARE_RECORD_GET(fdb_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_GET(inseg_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_GET(ipmc_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_GET(l2mc_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_GET(mcast_fdb_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_GET(neighbor_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_GET(route_entry);
            SAI_REDIS_RECORDER_DECLARE_RECORD_GET(nat_entry);


        public: // SAI stats API

            void recordGenericGetStats(
                    _In_ sai_object_type_t object_type,
                    _In_ sai_object_id_t object_id,
                    _In_ uint32_t number_of_counters,
                    _In_ const sai_stat_id_t *counter_ids);

            // TODO to private
            void recordGenericGetStats(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordGenericGetStatsResponse(
                    _In_ sai_status_t status,
                    _In_ uint32_t count,
                    _In_ const uint64_t *counters);

            void recordGenericClearStats(
                    _In_ sai_object_type_t object_type,
                    _In_ sai_object_id_t object_id,
                    _In_ uint32_t number_of_counters,
                    _In_ const sai_stat_id_t *counter_ids);

            // TODO to private
            void recordGenericClearStats(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordGenericClearStatsResponse(
                    _In_ sai_status_t status);

        public: // SAI bulk API

            void recordBulkGenericCreate(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordBulkGenericCreateResponse(
                    _In_ sai_status_t status,
                    _In_ uint32_t objectCount,
                    _In_ const sai_status_t *objectStatuses);

            void recordBulkGenericRemove(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordBulkGenericRemoveResponse(
                    _In_ sai_status_t status,
                    _In_ uint32_t objectCount,
                    _In_ const sai_status_t *objectStatuses);

            void recordBulkGenericSet(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordBulkGenericSetResponse(
                    _In_ sai_status_t status,
                    _In_ uint32_t objectCount,
                    _In_ const sai_status_t *objectStatuses);

        public: // SAI query interface API

            void recordFlushFdbEntries(
                    _In_ sai_object_id_t switchId,
                    _In_ uint32_t attrCount,
                    _In_ const sai_attribute_t *attrList);

            // TODO move to private
            void recordFlushFdbEntries(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordFlushFdbEntriesResponse(
                    _In_ sai_status_t status);

        public: // SAI global interface API

            void recordObjectTypeGetAvailability(
                    _In_ sai_object_id_t switchId,
                    _In_ sai_object_type_t objectType,
                    _In_ uint32_t attrCount,
                    _In_ const sai_attribute_t *attrList);

            void recordObjectTypeGetAvailabilityResponse(
                    _In_ sai_status_t status,
                    _In_ const uint64_t *count);

            void recordQueryAttributeCapability(
                    _In_ sai_object_id_t switch_id,
                    _In_ sai_object_type_t object_type,
                    _In_ sai_attr_id_t attr_id,
                    _Out_ sai_attr_capability_t *capability);

            void recordQueryAttributeCapabilityResponse(
                    _In_ sai_status_t status,
                    _In_ sai_object_type_t objectType,
                    _In_ sai_attr_id_t attrId,
                    _In_ const sai_attr_capability_t* capability);

            void recordQueryAattributeEnumValuesCapability(
                    _In_ sai_object_id_t switch_id,
                    _In_ sai_object_type_t object_type,
                    _In_ sai_attr_id_t attr_id,
                    _Inout_ sai_s32_list_t *enum_values_capability);

            void recordQueryAattributeEnumValuesCapabilityResponse(
                    _In_ sai_status_t status,
                    _In_ sai_object_type_t objectType,
                    _In_ sai_attr_id_t attrId,
                    _In_ const sai_s32_list_t* enumValuesCapability);

            // TODO move to private
            void recordQueryAttributeCapability(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordQueryAttributeCapabilityResponse(
                    _In_ sai_status_t status,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordQueryAttributeEnumValuesCapability(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordQueryAttributeEnumValuesCapabilityResponse(
                    _In_ sai_status_t status,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordObjectTypeGetAvailability(
                    _In_ const std::string& key,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

            void recordObjectTypeGetAvailabilityResponse(
                    _In_ sai_status_t status,
                    _In_ const std::vector<swss::FieldValueTuple>& arguments);

        public: // SAI notifications

            void recordNotification(
                    _In_ const std::string &name,
                    _In_ const std::string &serializedNotification,
                    _In_ const std::vector<swss::FieldValueTuple> &values);

        public: // sairedis/syncd internal API

            void recordNotifySyncd(
                    _In_ sai_object_id_t switchId,
                    _In_ sai_redis_notify_syncd_t redisNotifySyncd);

            void recordNotifySyncd(
                    _In_ const std::string& key);

            void recordNotifySyncdResponse(
                    _In_ sai_status_t status);

        public: // Recorder API

            void enableRecording(
                    _In_ bool enabled);

            bool setRecordingOutputDirectory(
                    _In_ const sai_attribute_t &attr);

            void requestLogRotate();

        public: // static helper functions

            static std::string getTimestamp();

            void recordStats(
                _In_ bool enable);

        private: // recording helpers

            void recordCreate(
                    _In_ sai_object_type_t objectType,
                    _In_ const std::string& serializedObjectId,
                    _In_ uint32_t attr_count,
                    _In_ const sai_attribute_t *attr_list);

            void recordRemove(
                    _In_ sai_object_type_t objectType,
                    _In_ const std::string& serializedObjectId);

            void recordSet(
                    _In_ sai_object_type_t objectType,
                    _In_ const std::string& serializedObjectId,
                    _In_ const sai_attribute_t *attr);

            void recordGet(
                    _In_ sai_object_type_t object_type,
                    _In_ const std::string &serializedObjectId,
                    _In_ uint32_t attr_count,
                    _In_ const sai_attribute_t *attr_list);

        private:

            void recordingFileReopen();

            void startRecording();

            void stopRecording();

            void recordLine(
                    _In_ const std::string& line);


        private:

            bool m_performLogRotate;

            bool m_enabled;

            bool m_recordStats;

            std::string m_recordingOutputDirectory;

            std::string m_recordingFileName;

            std::string m_recordingFile;

            std::ofstream m_ofstream;

            std::mutex m_mutex;
    };
}
