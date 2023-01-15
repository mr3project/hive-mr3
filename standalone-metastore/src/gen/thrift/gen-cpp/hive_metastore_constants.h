/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#ifndef hive_metastore_CONSTANTS_H
#define hive_metastore_CONSTANTS_H

#include "hive_metastore_types.h"

namespace Apache { namespace Hadoop { namespace Hive {

class hive_metastoreConstants {
 public:
  hive_metastoreConstants();

  std::string DDL_TIME;
  std::string HIVE_FILTER_FIELD_OWNER;
  std::string HIVE_FILTER_FIELD_PARAMS;
  std::string HIVE_FILTER_FIELD_LAST_ACCESS;
  std::string IS_ARCHIVED;
  std::string ORIGINAL_LOCATION;
  std::string IS_IMMUTABLE;
  std::string META_TABLE_COLUMNS;
  std::string META_TABLE_COLUMN_TYPES;
  std::string BUCKET_FIELD_NAME;
  std::string BUCKET_COUNT;
  std::string FIELD_TO_DIMENSION;
  std::string META_TABLE_NAME;
  std::string META_TABLE_DB;
  std::string META_TABLE_LOCATION;
  std::string META_TABLE_SERDE;
  std::string META_TABLE_PARTITION_COLUMNS;
  std::string META_TABLE_PARTITION_COLUMN_TYPES;
  std::string FILE_INPUT_FORMAT;
  std::string FILE_OUTPUT_FORMAT;
  std::string META_TABLE_STORAGE;
  std::string TABLE_IS_TRANSACTIONAL;
  std::string TABLE_NO_AUTO_COMPACT;
  std::string TABLE_TRANSACTIONAL_PROPERTIES;
  std::string TABLE_BUCKETING_VERSION;
  std::string EXPECTED_PARAMETER_KEY;
  std::string EXPECTED_PARAMETER_VALUE;
};

extern const hive_metastoreConstants g_hive_metastore_constants;

}}} // namespace

#endif
