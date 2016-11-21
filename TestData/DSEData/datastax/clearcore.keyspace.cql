
CREATE KEYSPACE clearcore WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'us-west-2-ccp': '3'
};

USE clearcore;

CREATE TABLE actions_organization_count (
  session_action text,
  user_id bigint,
  date int,
  second int,
  action_count counter,
  PRIMARY KEY ((session_action), user_id, date, second)
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

CREATE TABLE actions_session (
  session_id bigint,
  id bigint,
  action text,
  PRIMARY KEY ((session_id), id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE appraisal_ids_property (
  property_id bigint,
  id bigint,
  PRIMARY KEY ((property_id), id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE appraisals (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE appraisals_organization_ref (
  organization_key text,
  label text,
  value text,
  appraisal_id bigint,
  appraisal_update_id bigint,
  PRIMARY KEY ((organization_key, label, value), appraisal_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE asc_licenses (
  id bigint,
  license_expiry_date int,
  license_number text,
  license_state text,
  license_status text,
  license_type text,
  licensee_city text,
  licensee_company text,
  licensee_county text,
  licensee_county_code text,
  licensee_first_name text,
  licensee_full_name text,
  licensee_last_name text,
  licensee_middle_name text,
  licensee_name_suffix text,
  licensee_phone text,
  licensee_state text,
  licensee_street text,
  licensee_zip text,
  solr_query text,
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

CREATE CUSTOM INDEX clearcore_asc_licenses_license_expiry_date_index ON asc_licenses (license_expiry_date) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_license_number_index ON asc_licenses (license_number) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_license_state_index ON asc_licenses (license_state) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_license_status_index ON asc_licenses (license_status) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_license_type_index ON asc_licenses (license_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_city_index ON asc_licenses (licensee_city) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_company_index ON asc_licenses (licensee_company) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_county_index ON asc_licenses (licensee_county) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_county_code_index ON asc_licenses (licensee_county_code) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_first_name_index ON asc_licenses (licensee_first_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_full_name_index ON asc_licenses (licensee_full_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_last_name_index ON asc_licenses (licensee_last_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_middle_name_index ON asc_licenses (licensee_middle_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_name_suffix_index ON asc_licenses (licensee_name_suffix) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_phone_index ON asc_licenses (licensee_phone) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_state_index ON asc_licenses (licensee_state) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_street_index ON asc_licenses (licensee_street) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_licensee_zip_index ON asc_licenses (licensee_zip) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_asc_licenses_solr_query_index ON asc_licenses (solr_query) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE TABLE case_events (
  case_id bigint,
  id bigint,
  json text,
  triggering_user_id bigint,
  PRIMARY KEY ((case_id), id)
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

CREATE TABLE case_review_form_versions (
  id bigint,
  update_id bigint,
  description text,
  form_definition map<text, text>,
  name text,
  organization_key text,
  PRIMARY KEY ((id), update_id)
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

CREATE TABLE case_review_forms (
  organization_key text,
  id bigint,
  description text,
  form_definition map<text, text>,
  name text,
  update_id bigint,
  PRIMARY KEY ((organization_key), id)
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

CREATE TABLE case_sets (
  id bigint,
  case_set_criteria text,
  case_set_filter_criteria text,
  case_set_flags set<text>,
  name text,
  organization_key text,
  owner_user_id bigint,
  status text,
  update_id bigint,
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

CREATE TABLE case_sets_organization (
  organization_key text,
  id bigint,
  json text,
  status text,
  PRIMARY KEY ((organization_key), id)
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

CREATE TABLE caselabels (
  id bigint,
  color text,
  description text,
  display_name text,
  organization_key text,
  status text,
  upper_case_name text,
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

CREATE TABLE caselabels_organization (
  organization_key text,
  upper_case_name text,
  id bigint,
  color text,
  description text,
  display_name text,
  status text,
  PRIMARY KEY ((organization_key), upper_case_name, id)
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

CREATE TABLE cases_document (
  document_id bigint,
  case_id bigint,
  json text,
  PRIMARY KEY ((document_id), case_id)
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

CREATE TABLE cases_organization_ref (
  organization_key text,
  label text,
  value text,
  case_id bigint,
  case_update_id bigint,
  PRIMARY KEY ((organization_key, label, value), case_id)
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

CREATE TABLE clear_re_responses (
  id bigint,
  validation_id bigint,
  json text,
  PRIMARY KEY ((id), validation_id)
) WITH COMPACT STORAGE AND
  CLUSTERING ORDER BY (validation_id DESC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='NONE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'} AND
  compression={'chunk_length_kb': '64', 'sstable_compression': 'LZ4Compressor'};

CREATE TABLE collateral_analytics_responses (
  id bigint,
  json text,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE comp_reports (
  id bigint,
  json text,
  PRIMARY KEY ((id))
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

CREATE TABLE comps_id (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE configurations_org_key (
  org_key text,
  configuration_key text,
  value text,
  PRIMARY KEY ((org_key), configuration_key)
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

CREATE TABLE configurations_user (
  user_id bigint,
  configuration_key text,
  value text,
  PRIMARY KEY ((user_id), configuration_key)
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

CREATE TABLE core_cases_caselabel (
  display_name text,
  organization_key text,
  id bigint,
  update_id bigint,
  PRIMARY KEY ((display_name, organization_key), id)
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

CREATE TABLE core_cases_property (
  property_id bigint,
  id bigint,
  json text,
  PRIMARY KEY ((property_id), id)
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

CREATE TABLE core_cases_search (
  id bigint,
  assigned_labels set<text>,
  assigned_labels_assigned_time set<text>,
  assigned_labels_removed_time set<text>,
  assigned_to bigint,
  assigned_to_display_name text,
  assigned_to_initials text,
  case_id_any_ngram text,
  case_id_copy text,
  case_id_edge text,
  case_id_ngram text,
  case_id_token text,
  completed_by bigint,
  conclusion bigint,
  copyfield_org_ref_any_ngram set<text>,
  copyfield_org_ref_copy set<text>,
  copyfield_org_ref_edge set<text>,
  copyfield_org_ref_ngram set<text>,
  copyfield_org_ref_token set<text>,
  description text,
  due_date timestamp,
  form_data_ map<text, text>,
  form_id bigint,
  form_update_id bigint,
  label_overrides set<text>,
  last_status_update timestamp,
  max_variance double,
  org_ref_ map<text, text>,
  organization_key text,
  previous_case_id bigint,
  previous_conclusion bigint,
  property_address_city text,
  property_address_country text,
  property_address_county text,
  property_address_full text,
  property_address_full_any_ngram text,
  property_address_full_copy text,
  property_address_full_edge text,
  property_address_full_ngram text,
  property_address_full_ngram_with_synonyms text,
  property_address_full_token text,
  property_address_full_token_with_synonyms text,
  property_address_source_id bigint,
  property_address_source_name text,
  property_address_state text,
  property_address_street text,
  property_address_street_any_ngram text,
  property_address_street_copy text,
  property_address_street_edge text,
  property_address_street_ngram text,
  property_address_street_ngram_with_synonyms text,
  property_address_street_token text,
  property_address_street_token_with_synonyms text,
  property_address_unit_number text,
  property_address_zipcode text,
  property_geocode_type text,
  property_geocoder_source_id bigint,
  property_geocoder_source_name text,
  property_id bigint,
  property_lat double,
  property_lng double,
  property_neighborhood text,
  property_parcel_apn text,
  property_tax_id text,
  property_update_id bigint,
  solr_query text,
  status text,
  tags set<text>,
  update_id bigint,
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

CREATE CUSTOM INDEX clearcore_core_cases_search_assigned_labels_index ON core_cases_search (assigned_labels) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_assigned_labels_assigned_time_index ON core_cases_search (assigned_labels_assigned_time) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_assigned_labels_removed_time_index ON core_cases_search (assigned_labels_removed_time) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_assigned_to_index ON core_cases_search (assigned_to) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_assigned_to_display_name_index ON core_cases_search (assigned_to_display_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_assigned_to_initials_index ON core_cases_search (assigned_to_initials) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_case_id_any_ngram_index ON core_cases_search (case_id_any_ngram) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_case_id_copy_index ON core_cases_search (case_id_copy) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_case_id_edge_index ON core_cases_search (case_id_edge) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_case_id_ngram_index ON core_cases_search (case_id_ngram) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_case_id_token_index ON core_cases_search (case_id_token) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_completed_by_index ON core_cases_search (completed_by) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_conclusion_index ON core_cases_search (conclusion) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_copyfield_org_ref_any_ngram_index ON core_cases_search (copyfield_org_ref_any_ngram) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_copyfield_org_ref_copy_index ON core_cases_search (copyfield_org_ref_copy) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_copyfield_org_ref_edge_index ON core_cases_search (copyfield_org_ref_edge) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_copyfield_org_ref_ngram_index ON core_cases_search (copyfield_org_ref_ngram) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_copyfield_org_ref_token_index ON core_cases_search (copyfield_org_ref_token) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_description_index ON core_cases_search (description) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_due_date_index ON core_cases_search (due_date) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_form_id_index ON core_cases_search (form_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_form_update_id_index ON core_cases_search (form_update_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_label_overrides_index ON core_cases_search (label_overrides) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_last_status_update_index ON core_cases_search (last_status_update) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_max_variance_index ON core_cases_search (max_variance) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_org_ref__index ON core_cases_search (org_ref_) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_organization_key_index ON core_cases_search (organization_key) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_previous_conclusion_index ON core_cases_search (previous_conclusion) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_city_index ON core_cases_search (property_address_city) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_country_index ON core_cases_search (property_address_country) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_county_index ON core_cases_search (property_address_county) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_full_index ON core_cases_search (property_address_full) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_full_any_ngram_index ON core_cases_search (property_address_full_any_ngram) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_full_copy_index ON core_cases_search (property_address_full_copy) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_full_edge_index ON core_cases_search (property_address_full_edge) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_full_ngram_index ON core_cases_search (property_address_full_ngram) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_full_ngram_with_synonyms_index ON core_cases_search (property_address_full_ngram_with_synonyms) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_full_token_index ON core_cases_search (property_address_full_token) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_full_token_with_synonyms_index ON core_cases_search (property_address_full_token_with_synonyms) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_source_id_index ON core_cases_search (property_address_source_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_source_name_index ON core_cases_search (property_address_source_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_state_index ON core_cases_search (property_address_state) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_street_index ON core_cases_search (property_address_street) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_street_any_ngram_index ON core_cases_search (property_address_street_any_ngram) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_street_copy_index ON core_cases_search (property_address_street_copy) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_street_edge_index ON core_cases_search (property_address_street_edge) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_street_ngram_index ON core_cases_search (property_address_street_ngram) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_street_ngram_with_synonyms_index ON core_cases_search (property_address_street_ngram_with_synonyms) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_street_token_index ON core_cases_search (property_address_street_token) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_street_token_with_synonyms_index ON core_cases_search (property_address_street_token_with_synonyms) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_unit_number_index ON core_cases_search (property_address_unit_number) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_address_zipcode_index ON core_cases_search (property_address_zipcode) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_geocode_type_index ON core_cases_search (property_geocode_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_geocoder_source_id_index ON core_cases_search (property_geocoder_source_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_geocoder_source_name_index ON core_cases_search (property_geocoder_source_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_id_index ON core_cases_search (property_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_lat_index ON core_cases_search (property_lat) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_lng_index ON core_cases_search (property_lng) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_neighborhood_index ON core_cases_search (property_neighborhood) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_parcel_apn_index ON core_cases_search (property_parcel_apn) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_tax_id_index ON core_cases_search (property_tax_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_property_update_id_index ON core_cases_search (property_update_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_solr_query_index ON core_cases_search (solr_query) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_status_index ON core_cases_search (status) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_tags_index ON core_cases_search (tags) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_core_cases_search_update_id_index ON core_cases_search (update_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE TABLE core_cases_version (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
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

CREATE TABLE documents (
  id bigint,
  update_id bigint,
  doc blob,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE documents_case (
  id bigint,
  document_id bigint,
  document_status text,
  document_type text,
  json text,
  organization_key text,
  PRIMARY KEY ((id), document_id)
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

CREATE TABLE documents_info (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE entity_validators (
  entity text,
  json text,
  PRIMARY KEY ((entity))
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE eventlogs (
  id bigint,
  api_user_id bigint,
  app_user_id bigint,
  asset_ids list<text>,
  asset_update_ids list<text>,
  http_method text,
  level text,
  message text,
  resource_uri text,
  run_as_user_id bigint,
  session_id bigint,
  stack_trace text,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE eventlogs_asset (
  asset_id bigint,
  asset_update_id text,
  eventlog_id bigint,
  PRIMARY KEY ((asset_id), asset_update_id, eventlog_id)
) WITH COMPACT STORAGE AND
  CLUSTERING ORDER BY (asset_update_id DESC, eventlog_id DESC) AND
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE eventlogs_session (
  session_id bigint,
  eventlog_id bigint,
  json text,
  PRIMARY KEY ((session_id), eventlog_id)
) WITH COMPACT STORAGE AND
  CLUSTERING ORDER BY (eventlog_id DESC) AND
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE events_transaction (
  transaction_id bigint,
  id bigint,
  json text,
  PRIMARY KEY ((transaction_id), id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE field_validators (
  name text,
  json text,
  PRIMARY KEY ((name))
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE hve (
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

CREATE TABLE jobs (
  id bigint,
  job_type text,
  json text,
  status text,
  update_id bigint,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE label_definitions (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
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

CREATE TABLE label_definitions_organization (
  organization_key text,
  id bigint,
  json text,
  PRIMARY KEY ((organization_key), id)
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

CREATE TABLE label_definitions_organization_by_name (
  organization_key text,
  name text,
  json text,
  PRIMARY KEY ((organization_key), name)
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

CREATE TABLE labels_organization (
  organization_key text,
  label text,
  PRIMARY KEY ((organization_key), label)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE licenses (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE mls_records (
  id bigint,
  json text,
  organization_key text,
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

CREATE TABLE mls_records_property (
  property_id bigint,
  id bigint,
  json text,
  organization_key text,
  PRIMARY KEY ((property_id), id)
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

CREATE TABLE organization_providers (
  organization_key text,
  provider_id bigint,
  PRIMARY KEY ((organization_key), provider_id)
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

CREATE TABLE organizations_key (
  key text,
  update_id bigint,
  id bigint,
  name text,
  role_names set<text>,
  status text,
  PRIMARY KEY ((key), update_id)
) WITH CLUSTERING ORDER BY (update_id DESC) AND
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE passcodes_user (
  userid bigint,
  id bigint,
  enabled boolean,
  passcode int,
  type text,
  PRIMARY KEY ((userid), id)
) WITH CLUSTERING ORDER BY (id DESC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE people (
  id bigint,
  company_name text,
  display_name text,
  email text,
  first_name text,
  initials text,
  last_name text,
  organization_key text,
  phone text,
  role text,
  source_id bigint,
  source_name text,
  update_id bigint,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE people_version (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE preferences_org_key (
  org_key text,
  preference_key text,
  value text,
  PRIMARY KEY ((org_key), preference_key)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE preferences_user (
  user_id bigint,
  preference_key text,
  value text,
  PRIMARY KEY ((user_id), preference_key)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE properties (
  id bigint,
  address_source_id bigint,
  address_source_name text,
  city text,
  country text,
  county text,
  geocode_source_id bigint,
  geocode_source_name text,
  geocode_type text,
  geolocation text,
  latitude double,
  longitude double,
  neighborhood text,
  parcel_apn text,
  solr_query text,
  state text,
  street text,
  tax_id text,
  unit_number text,
  update_id bigint,
  zipcode text,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE CUSTOM INDEX clearcore_properties_address_source_id_index ON properties (address_source_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_address_source_name_index ON properties (address_source_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_city_index ON properties (city) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_country_index ON properties (country) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_county_index ON properties (county) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_geocode_source_id_index ON properties (geocode_source_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_geocode_source_name_index ON properties (geocode_source_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_geocode_type_index ON properties (geocode_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_geolocation_index ON properties (geolocation) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_latitude_index ON properties (latitude) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_longitude_index ON properties (longitude) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_neighborhood_index ON properties (neighborhood) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_parcel_apn_index ON properties (parcel_apn) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_solr_query_index ON properties (solr_query) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_state_index ON properties (state) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_street_index ON properties (street) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_tax_id_index ON properties (tax_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_unit_number_index ON properties (unit_number) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_update_id_index ON properties (update_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_properties_zipcode_index ON properties (zipcode) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE TABLE properties_version (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE properties_zipcode_street_unit (
  zipcode text,
  street text,
  unit_number text,
  address_source_id bigint,
  address_source_name text,
  city text,
  country text,
  county text,
  geocode_source_id bigint,
  geocode_source_name text,
  geocode_type text,
  id bigint,
  latitude double,
  longitude double,
  neighborhood text,
  parcel_apn text,
  state text,
  tax_id text,
  update_id bigint,
  PRIMARY KEY ((zipcode, street, unit_number))
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE property_activity_valuations (
  mp_activity_id bigint,
  valuation_id bigint,
  property_activity_id bigint,
  PRIMARY KEY ((mp_activity_id), valuation_id, property_activity_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE provider_organizations (
  provider_id bigint,
  organization_key text,
  PRIMARY KEY ((provider_id), organization_key)
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

CREATE TABLE queues (
  id bigint,
  organization_key text,
  update_id bigint,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE queues_organization (
  organization_key text,
  id bigint,
  json text,
  PRIMARY KEY ((organization_key), id)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE roles (
  id bigint,
  description text,
  display_name text,
  organization_key text,
  permissions set<text>,
  role_name text,
  role_names set<text>,
  status text,
  type text,
  update_id bigint,
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

CREATE TABLE roles_name (
  role_name text,
  update_id bigint,
  description text,
  display_name text,
  id bigint,
  organization_key text,
  permissions set<text>,
  role_names set<text>,
  status text,
  type text,
  PRIMARY KEY ((role_name), update_id)
) WITH CLUSTERING ORDER BY (update_id DESC) AND
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

CREATE TABLE roles_organization (
  organization_key text,
  id bigint,
  json text,
  PRIMARY KEY ((organization_key), id)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE roles_version (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE rule_based_analyses (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
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

CREATE TABLE rule_based_analyses_asset (
  asset_id bigint,
  asset_update_id bigint,
  rule_based_analysis_id bigint,
  json text,
  PRIMARY KEY ((asset_id), asset_update_id, rule_based_analysis_id)
) WITH COMPACT STORAGE AND
  CLUSTERING ORDER BY (asset_update_id DESC, rule_based_analysis_id DESC) AND
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
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE sessions (
  id bigint,
  permissions list<text>,
  person_company_name text,
  person_display_name text,
  person_email text,
  person_first_name text,
  person_id bigint,
  person_initials text,
  person_last_name text,
  person_organization_key text,
  person_phone text,
  person_role text,
  person_source_id bigint,
  person_source_name text,
  person_update_id bigint,
  real_person_company_name text,
  real_person_display_name text,
  real_person_email text,
  real_person_first_name text,
  real_person_id bigint,
  real_person_initials text,
  real_person_last_name text,
  real_person_organization_key text,
  real_person_phone text,
  real_person_role text,
  real_person_source_id bigint,
  real_person_source_name text,
  real_person_update_id bigint,
  real_user_id bigint,
  real_user_key text,
  real_user_organization_key text,
  real_user_password_update_id bigint,
  real_user_permissions list<text>,
  real_user_roles set<text>,
  real_user_status text,
  real_user_type text,
  real_user_update_id bigint,
  type text,
  user_id bigint,
  user_key text,
  user_organization_key text,
  user_password_update_id bigint,
  user_permissions list<text>,
  user_roles set<text>,
  user_status text,
  user_type text,
  user_update_id bigint,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE sessions_user (
  user_id bigint,
  session_id bigint,
  actions counter,
  PRIMARY KEY ((user_id), session_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE sources (
  source_name text,
  date int,
  id bigint,
  PRIMARY KEY ((source_name, date), id)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE sources_v3 (
  source_name text,
  organization_key text,
  date int,
  id bigint,
  duration bigint,
  json text,
  PRIMARY KEY ((source_name, organization_key, date), id)
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

CREATE TABLE tokens_job (
  job_id bigint,
  token_id bigint,
  PRIMARY KEY ((job_id), token_id)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE transaction_durations_org_date (
  organization_key text,
  date int,
  id bigint,
  duration bigint,
  PRIMARY KEY ((organization_key, date), id)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE transaction_errors_org_date (
  organization_key text,
  date int,
  error_code text,
  id bigint,
  json text,
  PRIMARY KEY ((organization_key, date), error_code, id)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE transactions (
  id bigint,
  billable boolean,
  day_of_month int,
  errorcode text,
  hour_of_day int,
  month int,
  organization_counts map<text, bigint>,
  organization_key text,
  organization_tags map<text, text>,
  organization_transaction_id text,
  year int,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE transactions_org_date (
  organization_key text,
  date int,
  id bigint,
  json text,
  retry_counter int,
  PRIMARY KEY ((organization_key, date), id)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE transactions_organization_transaction_id (
  organization_transaction_id text,
  id bigint,
  json text,
  PRIMARY KEY ((organization_transaction_id), id)
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE users (
  id bigint,
  company_name text,
  display_name text,
  email text,
  first_name text,
  initials text,
  key text,
  last_name text,
  organization_key text,
  password text,
  password_update_id bigint,
  person_id bigint,
  person_organization_key text,
  person_source_id bigint,
  person_source_name text,
  person_update_id bigint,
  phone text,
  role text,
  roles set<text>,
  status text,
  type text,
  update_id bigint,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE users_key (
  key text,
  company_name text,
  display_name text,
  email text,
  first_name text,
  id bigint,
  initials text,
  last_name text,
  organization_key text,
  password text,
  password_update_id bigint,
  person_id bigint,
  person_organization_key text,
  person_source_id bigint,
  person_source_name text,
  person_update_id bigint,
  phone text,
  role text,
  roles set<text>,
  status text,
  type text,
  update_id bigint,
  PRIMARY KEY ((key))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE users_organization (
  organization_key text,
  user_id bigint,
  user_key text,
  PRIMARY KEY ((organization_key), user_id)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE users_person (
  person_id bigint,
  user_id bigint,
  PRIMARY KEY ((person_id), user_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE users_version (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE validation_rule_sets_key (
  validation_rule_set_key text,
  validation_rule_set_id bigint,
  PRIMARY KEY ((validation_rule_set_key))
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE validation_rule_sets_organization (
  organization_key text,
  validation_rule_set_id bigint,
  PRIMARY KEY ((organization_key))
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE validation_rule_sets_organization_v2 (
  organization_key text,
  validation_rule_set_id bigint,
  PRIMARY KEY ((organization_key), validation_rule_set_id)
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

CREATE TABLE validation_rule_sets_version (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH CLUSTERING ORDER BY (update_id DESC) AND
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE validation_sets_version (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE validations (
  id bigint,
  json text,
  PRIMARY KEY ((id))
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE validations_appraisal (
  appraisal_id bigint,
  appraisal_update_id bigint,
  validation_id bigint,
  json text,
  PRIMARY KEY ((appraisal_id), appraisal_update_id, validation_id)
) WITH COMPACT STORAGE AND
  CLUSTERING ORDER BY (appraisal_update_id DESC, validation_id DESC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE validations_property (
  property_id bigint,
  validation_id bigint,
  json text,
  PRIMARY KEY ((property_id), validation_id)
) WITH COMPACT STORAGE AND
  CLUSTERING ORDER BY (validation_id DESC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE validations_search (
  validation_id bigint,
  appraisal_address text,
  appraisal_address_city text,
  appraisal_address_state text,
  appraisal_address_street text,
  appraisal_address_unified text,
  appraisal_address_unitnumber text,
  appraisal_address_zipcode text,
  appraisal_appraiser_license_identifier text,
  appraisal_appraiser_license_state text,
  appraisal_appraiser_name text,
  appraisal_appraiser_unified text,
  appraisal_bedroom_count int,
  appraisal_borrower_name text,
  appraisal_effective_date int,
  appraisal_floor_number text,
  appraisal_gla double,
  appraisal_neighborhood_name text,
  appraisal_property_uad_condition text,
  appraisal_purpose_type text,
  appraisal_quality_of_construction text,
  appraisal_reconciliation_market_price int,
  appraisal_story_count int,
  appraisal_submission_date int,
  appraisal_type_data_standard text,
  appraisal_type_form text,
  appraisal_type_software_vendor text,
  appraisal_type_xml_format text,
  appraisal_type_xml_format_version text,
  appraisal_year_built int,
  general_search text,
  is_comps_ranked boolean,
  numeric_score map<text, double>,
  organization_key text,
  organization_reference map<text, text>,
  rule_key map<text, text>,
  rule_key_set set<text>,
  rule_max_weight map<text, double>,
  rule_max_weight_set set<double>,
  rule_min_weight map<text, double>,
  rule_min_weight_set set<double>,
  rule_name map<text, text>,
  rule_name_set set<text>,
  rule_result_category_description map<text, text>,
  rule_result_category_description_set set<text>,
  rule_result_is_critical map<text, boolean>,
  rule_result_is_critical_set set<boolean>,
  rule_result_result map<text, text>,
  rule_result_result_set set<text>,
  rule_result_score_description map<text, text>,
  rule_result_score_description_set set<text>,
  rule_result_weight map<text, double>,
  rule_result_weight_set set<double>,
  rule_set_key text,
  rule_set_name text,
  rule_set_version_key text,
  rule_version_key map<text, text>,
  rule_version_key_set set<text>,
  score_key map<text, text>,
  score_key_set set<text>,
  score_name map<text, text>,
  score_name_set set<text>,
  score_score map<text, text>,
  solr_query text,
  validated_asset_id bigint,
  validated_asset_update_id bigint,
  validation_submission_date int,
  valuation_accuracy_confidence_level text,
  valuation_accuracy_score double,
  valuation_analysis_delta double,
  valuation_analysis_is_over boolean,
  valuation_analysis_is_under boolean,
  valuation_analysis_max_amount double,
  valuation_analysis_min_amount double,
  valuation_analysis_report_value int,
  valuation_analysis_risk text,
  valuation_property_sub_type text,
  valuation_quality_confidence_level text,
  valuation_quality_score double,
  PRIMARY KEY ((validation_id))
) WITH
  bloom_filter_fp_chance=0.100000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_address_index ON validations_search (appraisal_address) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_address_city_index ON validations_search (appraisal_address_city) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_address_state_index ON validations_search (appraisal_address_state) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_address_street_index ON validations_search (appraisal_address_street) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_address_unified_index ON validations_search (appraisal_address_unified) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_address_unitnumber_index ON validations_search (appraisal_address_unitnumber) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_address_zipcode_index ON validations_search (appraisal_address_zipcode) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_appraiser_license_identifier_index ON validations_search (appraisal_appraiser_license_identifier) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_appraiser_license_state_index ON validations_search (appraisal_appraiser_license_state) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_appraiser_name_index ON validations_search (appraisal_appraiser_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_appraiser_unified_index ON validations_search (appraisal_appraiser_unified) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_bedroom_count_index ON validations_search (appraisal_bedroom_count) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_borrower_name_index ON validations_search (appraisal_borrower_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_effective_date_index ON validations_search (appraisal_effective_date) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_floor_number_index ON validations_search (appraisal_floor_number) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_gla_index ON validations_search (appraisal_gla) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_neighborhood_name_index ON validations_search (appraisal_neighborhood_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_property_uad_condition_index ON validations_search (appraisal_property_uad_condition) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_purpose_type_index ON validations_search (appraisal_purpose_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_quality_of_construction_index ON validations_search (appraisal_quality_of_construction) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_reconciliation_market_price_index ON validations_search (appraisal_reconciliation_market_price) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_story_count_index ON validations_search (appraisal_story_count) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_submission_date_index ON validations_search (appraisal_submission_date) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_type_data_standard_index ON validations_search (appraisal_type_data_standard) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_type_form_index ON validations_search (appraisal_type_form) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_type_software_vendor_index ON validations_search (appraisal_type_software_vendor) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_type_xml_format_index ON validations_search (appraisal_type_xml_format) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_type_xml_format_version_index ON validations_search (appraisal_type_xml_format_version) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_appraisal_year_built_index ON validations_search (appraisal_year_built) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_general_search_index ON validations_search (general_search) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_is_comps_ranked_index ON validations_search (is_comps_ranked) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_numeric_score_index ON validations_search (numeric_score) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_organization_key_index ON validations_search (organization_key) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_organization_reference_index ON validations_search (organization_reference) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_key_index ON validations_search (rule_key) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_key_set_index ON validations_search (rule_key_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_max_weight_index ON validations_search (rule_max_weight) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_max_weight_set_index ON validations_search (rule_max_weight_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_min_weight_index ON validations_search (rule_min_weight) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_min_weight_set_index ON validations_search (rule_min_weight_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_name_index ON validations_search (rule_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_name_set_index ON validations_search (rule_name_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_result_category_description_index ON validations_search (rule_result_category_description) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_result_category_description_set_index ON validations_search (rule_result_category_description_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_result_is_critical_index ON validations_search (rule_result_is_critical) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_result_is_critical_set_index ON validations_search (rule_result_is_critical_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_result_result_index ON validations_search (rule_result_result) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_result_result_set_index ON validations_search (rule_result_result_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_result_score_description_index ON validations_search (rule_result_score_description) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_result_score_description_set_index ON validations_search (rule_result_score_description_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_result_weight_index ON validations_search (rule_result_weight) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_result_weight_set_index ON validations_search (rule_result_weight_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_set_key_index ON validations_search (rule_set_key) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_set_name_index ON validations_search (rule_set_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_set_version_key_index ON validations_search (rule_set_version_key) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_version_key_index ON validations_search (rule_version_key) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_rule_version_key_set_index ON validations_search (rule_version_key_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_score_key_index ON validations_search (score_key) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_score_key_set_index ON validations_search (score_key_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_score_name_index ON validations_search (score_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_score_name_set_index ON validations_search (score_name_set) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_score_score_index ON validations_search (score_score) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_solr_query_index ON validations_search (solr_query) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_validated_asset_id_index ON validations_search (validated_asset_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_validated_asset_update_id_index ON validations_search (validated_asset_update_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_validation_submission_date_index ON validations_search (validation_submission_date) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_valuation_accuracy_confidence_level_index ON validations_search (valuation_accuracy_confidence_level) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_valuation_accuracy_score_index ON validations_search (valuation_accuracy_score) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_valuation_analysis_delta_index ON validations_search (valuation_analysis_delta) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_valuation_analysis_is_over_index ON validations_search (valuation_analysis_is_over) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_valuation_analysis_max_amount_index ON validations_search (valuation_analysis_max_amount) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_valuation_analysis_min_amount_index ON validations_search (valuation_analysis_min_amount) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_valuation_analysis_report_value_index ON validations_search (valuation_analysis_report_value) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_valuation_analysis_risk_index ON validations_search (valuation_analysis_risk) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_valuation_property_sub_type_index ON validations_search (valuation_property_sub_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_valuation_quality_confidence_level_index ON validations_search (valuation_quality_confidence_level) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_validations_search_valuation_quality_score_index ON validations_search (valuation_quality_score) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE TABLE valuations (
  id bigint,
  geolocation text,
  organization_key text,
  property_id bigint,
  property_type text,
  solr_query text,
  source_name text,
  source_type text,
  source_type_ordinal int,
  update_id bigint,
  valuation_date int,
  valuation_type text,
  value int,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE CUSTOM INDEX clearcore_valuations_geolocation_index ON valuations (geolocation) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_valuations_organization_key_index ON valuations (organization_key) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_valuations_property_id_index ON valuations (property_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_valuations_property_type_index ON valuations (property_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_valuations_solr_query_index ON valuations (solr_query) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_valuations_source_name_index ON valuations (source_name) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_valuations_source_type_index ON valuations (source_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_valuations_source_type_ordinal_index ON valuations (source_type_ordinal) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_valuations_valuation_date_index ON valuations (valuation_date) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_valuations_valuation_type_index ON valuations (valuation_type) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE CUSTOM INDEX clearcore_valuations_value_index ON valuations (value) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

CREATE TABLE valuations_appraisal (
  source_id bigint,
  source_update_id bigint,
  id bigint,
  address_source_id bigint,
  address_source_name text,
  basement_finish_size int,
  basement_size int,
  basement_type text,
  bathroom_count int,
  bedroom_count int,
  building_size int,
  car_parking_count int,
  city text,
  condition text,
  construction_quality text,
  cooling_type text,
  country text,
  county text,
  days_on_market int,
  effective_year_built int,
  fema_flood_zone_indicator boolean,
  fireplace_indicator boolean,
  garage_type text,
  geocode_source_id bigint,
  geocode_source_name text,
  geocode_type text,
  half_bathroom_count int,
  heating_type text,
  historical_sources text,
  latitude double,
  loan_type text,
  longitude double,
  market_type text,
  neighborhood text,
  neighborhood_location_type text,
  neighborhood_name text,
  occupancy_type text,
  organization_key text,
  owner2_name text,
  owner_address_source_id bigint,
  owner_address_source_name text,
  owner_city text,
  owner_country text,
  owner_county text,
  owner_geocode_source_id bigint,
  owner_geocode_source_name text,
  owner_geocode_type text,
  owner_latitude double,
  owner_longitude double,
  owner_name text,
  owner_neighborhood text,
  owner_parcel_apn text,
  owner_property_id bigint,
  owner_property_update_id bigint,
  owner_state text,
  owner_street text,
  owner_tax_id text,
  owner_type text,
  owner_unit_number text,
  owner_vesting text,
  owner_zipcode text,
  parcel_apn text,
  pool_indicator boolean,
  porch_indicator boolean,
  property_id bigint,
  property_style text,
  property_sub_type text,
  property_type text,
  property_update_id bigint,
  pud_indicator boolean,
  room_count int,
  site_size int,
  source_comp int,
  source_name text,
  source_type text,
  state text,
  story_count int,
  street text,
  tax_id text,
  unit_count int,
  unit_number text,
  update_id bigint,
  valuation_date int,
  valuation_type text,
  value int,
  view_rating text,
  year_built int,
  zipcode text,
  PRIMARY KEY ((source_id), source_update_id, id)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE INDEX valuations_appraisal_source_comp ON valuations_appraisal (source_comp);

CREATE TABLE valuations_archive (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE valuations_json_property (
  property_id bigint,
  id bigint,
  valuation_json text,
  PRIMARY KEY ((property_id), id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE valuations_version (
  id bigint,
  update_id bigint,
  json text,
  PRIMARY KEY ((id), update_id)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE values_organization_label (
  organization_key text,
  label text,
  value text,
  appraisals counter,
  PRIMARY KEY ((organization_key, label), value)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

