-- RPPD PostgreSQL Extension
-- Version 0.2.2

-- Trigger function
CREATE OR REPLACE FUNCTION @extschema@.rppd_event()
RETURNS trigger
AS 'MODULE_PATHNAME', 'rppd_event'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION @extschema@.rppd_event() IS 'RPPD trigger function for table change notifications';

-- Status functions

-- Basic status (master node)
CREATE OR REPLACE FUNCTION @extschema@.rppd_info()
RETURNS text
AS 'MODULE_PATHNAME', 'rppd_info'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION @extschema@.rppd_info() IS 'Return node status in json format';

-- Status with config table override
CREATE OR REPLACE FUNCTION @extschema@.rppd_info(cfg text)
RETURNS text
AS 'MODULE_PATHNAME', 'rppd_info_cfg'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION @extschema@.rppd_info(cfg text) IS 'Use the config table name: The name includes schema, i.e.: schema.table. Default is "public.rppd_config". if ".<table>" than default use schema is "public". if "<schema>." than default use table is "rppd_config"';

-- Status for specific node
CREATE OR REPLACE FUNCTION @extschema@.rppd_info(node_id integer)
RETURNS text
AS 'MODULE_PATHNAME', 'rppd_info_node'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION @extschema@.rppd_info(node_id integer) IS 'Return node status in json format by node_id from rppd_config.id table';

-- Status for specific node with config table
CREATE OR REPLACE FUNCTION @extschema@.rppd_info(cfg text, node_id integer)
RETURNS text
AS 'MODULE_PATHNAME', 'rppd_info_node_cfg'
LANGUAGE C VOLATILE STRICT;

-- Status with function log ID
CREATE OR REPLACE FUNCTION @extschema@.rppd_info(node_id integer, fn_log_id bigint)
RETURNS text
AS 'MODULE_PATHNAME', 'rppd_info_id'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION @extschema@.rppd_info(node_id integer, fn_log_id bigint) IS 'Same as node status plus function execution status by fn_log.id table';

-- Status with function log ID and config table
CREATE OR REPLACE FUNCTION @extschema@.rppd_info(cfg text, node_id integer, fn_log_id bigint)
RETURNS text
AS 'MODULE_PATHNAME', 'rppd_info_id_cfg'
LANGUAGE C VOLATILE STRICT;

-- Status with function UUID
CREATE OR REPLACE FUNCTION @extschema@.rppd_info(node_id integer, fn_log_uuid text)
RETURNS text
AS 'MODULE_PATHNAME', 'rppd_info_uuid'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION @extschema@.rppd_info(node_id integer, fn_log_uuid text) IS 'Same as node status plus function execution status by uuid if not store to fn_log.id table. use * to get all uuid.';

-- Status with function UUID and config table
CREATE OR REPLACE FUNCTION @extschema@.rppd_info(cfg text, node_id integer, fn_log_uuid text)
RETURNS text
AS 'MODULE_PATHNAME', 'rppd_info_uuid_cfg'
LANGUAGE C VOLATILE STRICT;

-- Include setup tables and triggers
\ir pg_setup.sql
