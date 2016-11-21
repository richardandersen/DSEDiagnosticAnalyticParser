
CREATE KEYSPACE cma WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'us-west-2': '3'
};

USE cma;

CREATE TABLE brokerages_mls_feed (
  mls_feed_name text,
  member_id text,
  effective_date timestamp,
  brokerage_name text,
  id bigint,
  member_name text,
  termination_date timestamp,
  PRIMARY KEY ((mls_feed_name), member_id, effective_date)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE cma_reports (
  id bigint,
  report_key text,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE cma_reports_tracking_id (
  tracking_id text,
  id bigint,
  report_key text,
  PRIMARY KEY ((tracking_id), id)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE import_files_date (
  import_date int,
  import_file_name text,
  PRIMARY KEY ((import_date), import_file_name)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE import_files_mls_board_code (
  mls_board_code text,
  import_file_name text,
  PRIMARY KEY ((mls_board_code), import_file_name)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE mls_backfill_imports (
  mls_board_code text,
  mls_record_type text,
  mls_file_date text,
  mls_file_part int,
  backfill boolean,
  original_file_name text,
  PRIMARY KEY ((mls_board_code, mls_record_type), mls_file_date, mls_file_part)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE mls_codes_mls_name (
  mls_system_name text,
  mls_board_code text,
  PRIMARY KEY ((mls_system_name), mls_board_code)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE mls_feed_brokerage_contracts (
  id bigint,
  brokerage_name text,
  effective_date timestamp,
  member_id text,
  member_name text,
  mls_feed_name text,
  termination_date timestamp,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE mls_feeds_brokerage (
  member_id text,
  mls_feed_name text,
  effective_date timestamp,
  brokerage_name text,
  id bigint,
  member_name text,
  termination_date timestamp,
  PRIMARY KEY ((member_id), mls_feed_name, effective_date)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE mls_imports (
  mls_board_code text,
  mls_record_type text,
  mls_file_date text,
  mls_file_part int,
  backfill boolean,
  import_date int,
  original_file_name text,
  PRIMARY KEY ((mls_board_code, mls_record_type, mls_file_date), mls_file_part)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE mls_names_mls_code (
  mls_board_code text,
  mls_system_name text,
  PRIMARY KEY ((mls_board_code), mls_system_name)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE mls_recent_activity_property (
  property_id bigint,
  activity_date int,
  activity_entry_datetime timestamp,
  activity_type text,
  bathroom_count int,
  bedroom_count int,
  geohash_2 text,
  geohash_3 text,
  geohash_4 text,
  geohash_5 text,
  geohash_6 text,
  geohash_7 text,
  geohash_8 text,
  geohash_9 text,
  geolocation text,
  gla int,
  immed_mls_number text,
  immed_source_mls_name text,
  import_date int,
  import_file_name text,
  json text,
  listing_type text,
  mls_board_code text,
  property_type text,
  site_size int,
  solr_query text,
  year_built int,
  PRIMARY KEY ((property_id))
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE CUSTOM INDEX cma_mls_recent_activity_property_activity_date_index ON mls_recent_activity_property (activity_date) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_activity_entry_datetime_index ON mls_recent_activity_property (activity_entry_datetime) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_activity_type_index ON mls_recent_activity_property (activity_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_bathroom_count_index ON mls_recent_activity_property (bathroom_count) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_bedroom_count_index ON mls_recent_activity_property (bedroom_count) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_geohash_2_index ON mls_recent_activity_property (geohash_2) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_geohash_3_index ON mls_recent_activity_property (geohash_3) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_geohash_4_index ON mls_recent_activity_property (geohash_4) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_geohash_5_index ON mls_recent_activity_property (geohash_5) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_geohash_6_index ON mls_recent_activity_property (geohash_6) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_geohash_7_index ON mls_recent_activity_property (geohash_7) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_geohash_8_index ON mls_recent_activity_property (geohash_8) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_geohash_9_index ON mls_recent_activity_property (geohash_9) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_geolocation_index ON mls_recent_activity_property (geolocation) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_gla_index ON mls_recent_activity_property (gla) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_immed_mls_number_index ON mls_recent_activity_property (immed_mls_number) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_import_date_index ON mls_recent_activity_property (import_date) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_import_file_name_index ON mls_recent_activity_property (import_file_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_json_index ON mls_recent_activity_property (json) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_listing_type_index ON mls_recent_activity_property (listing_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_mls_board_code_index ON mls_recent_activity_property (mls_board_code) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_property_type_index ON mls_recent_activity_property (property_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_site_size_index ON mls_recent_activity_property (site_size) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_solr_query_index ON mls_recent_activity_property (solr_query) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_recent_activity_property_year_built_index ON mls_recent_activity_property (year_built) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE TABLE mls_records (
  id bigint,
  json text,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE mls_records_import_file (
  import_file_name text,
  mls_record_id bigint,
  PRIMARY KEY ((import_file_name), mls_record_id)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE mls_records_property (
  property_id bigint,
  immed_mls_number text,
  immed_source_mls_system_name text,
  activity_date int,
  activity_entry_datetime timestamp,
  activity_type text,
  activity_price int,
  bathroom_count int,
  bedroom_count int,
  geohash_2 text,
  geohash_3 text,
  geohash_4 text,
  geohash_5 text,
  geohash_6 text,
  geohash_7 text,
  geohash_8 text,
  geohash_9 text,
  geolocation text,
  gla int,
  immed_source_mls_system_name_exact text,
  import_date int,
  import_file_name text,
  json text,
  listing_type text,
  mls_board_code text,
  property_type text,
  site_size int,
  solr_query text,
  year_built int,
  PRIMARY KEY ((property_id), immed_mls_number, immed_source_mls_system_name, activity_date, activity_entry_datetime, activity_type)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE CUSTOM INDEX cma_mls_records_property_activity_price_index ON mls_records_property (activity_price) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_bathroom_count_index ON mls_records_property (bathroom_count) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_bedroom_count_index ON mls_records_property (bedroom_count) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_geohash_2_index ON mls_records_property (geohash_2) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_geohash_3_index ON mls_records_property (geohash_3) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_geohash_4_index ON mls_records_property (geohash_4) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_geohash_5_index ON mls_records_property (geohash_5) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_geohash_6_index ON mls_records_property (geohash_6) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_geohash_7_index ON mls_records_property (geohash_7) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_geohash_8_index ON mls_records_property (geohash_8) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_geohash_9_index ON mls_records_property (geohash_9) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_geolocation_index ON mls_records_property (geolocation) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_gla_index ON mls_records_property (gla) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_immed_source_mls_system_name_exact_index ON mls_records_property (immed_source_mls_system_name_exact) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_import_date_index ON mls_records_property (import_date) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_import_file_name_index ON mls_records_property (import_file_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_json_index ON mls_records_property (json) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_listing_type_index ON mls_records_property (listing_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_mls_board_code_index ON mls_records_property (mls_board_code) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_property_type_index ON mls_records_property (property_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_site_size_index ON mls_records_property (site_size) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_solr_query_index ON mls_records_property (solr_query) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX cma_mls_records_property_year_built_index ON mls_records_property (year_built) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE TABLE schema_version (
  date text,
  change text,
  hash text,
  version int,
  PRIMARY KEY ((date))
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

