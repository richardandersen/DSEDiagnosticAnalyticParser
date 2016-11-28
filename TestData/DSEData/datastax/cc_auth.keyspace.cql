
CREATE KEYSPACE cc_auth WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'us-west-2-ccp': '3'
};

USE cc_auth;

CREATE TABLE actions_organization_count (
  session_action text,
  user_id bigint,
  date int,
  second int,
  action_count counter,
  PRIMARY KEY ((session_action, user_id, date), second)
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

CREATE TABLE entity_validators (
  entity text,
  json text,
  PRIMARY KEY ((entity))
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

CREATE TABLE field_validators (
  name text,
  json text,
  PRIMARY KEY ((name))
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

CREATE TABLE passcodes_user (
  userid bigint,
  id bigint,
  enabled boolean,
  passcode int,
  type text,
  PRIMARY KEY ((userid), id)
) WITH CLUSTERING ORDER BY (id DESC) AND
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

CREATE TABLE people_version (
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

CREATE TABLE preferences_org_key (
  org_key text,
  preference_key text,
  value text,
  PRIMARY KEY ((org_key), preference_key)
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

CREATE TABLE preferences_user (
  user_id bigint,
  preference_key text,
  value text,
  PRIMARY KEY ((user_id), preference_key)
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

CREATE TABLE roles_version (
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
  real_user_roles set<text>,
  real_user_status text,
  real_user_type text,
  real_user_update_id bigint,
  type text,
  user_id bigint,
  user_key text,
  user_organization_key text,
  user_password_update_id bigint,
  user_roles set<text>,
  user_status text,
  user_type text,
  user_update_id bigint,
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

CREATE TABLE sessions_user (
  user_id bigint,
  session_id bigint,
  actions counter,
  PRIMARY KEY ((user_id), session_id)
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

CREATE TABLE users_organization (
  organization_key text,
  user_id bigint,
  json text,
  PRIMARY KEY ((organization_key), user_id)
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

CREATE TABLE users_person (
  person_id bigint,
  user_id bigint,
  PRIMARY KEY ((person_id), user_id)
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

CREATE TABLE users_version (
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

