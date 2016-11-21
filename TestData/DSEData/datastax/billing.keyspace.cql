
CREATE KEYSPACE billing WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'us-west-2': '3'
};

USE billing;

CREATE TABLE account_payables (
  date int,
  org_key text,
  product_type text,
  product_id bigint,
  product_update_id bigint,
  vendor_type text,
  parent_product_id bigint,
  parent_product_type text,
  parent_product_update_id bigint,
  user_id bigint,
  vendor_detail text,
  PRIMARY KEY ((date, org_key), product_type, product_id, product_update_id, vendor_type)
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

CREATE TABLE account_receivables (
  date int,
  org_key text,
  product_type text,
  product_id bigint,
  product_update_id bigint,
  json text,
  PRIMARY KEY ((date, org_key), product_type, product_id, product_update_id)
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

