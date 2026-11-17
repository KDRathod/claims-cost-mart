-- =============================================================================
-- Snowflake one-time setup for claims-cost-mart
-- Run once as ACCOUNTADMIN in a worksheet. Safe to re-run (IF NOT EXISTS / OR REPLACE).
--
-- What this creates, and why:
--   * One X-Small warehouse that suspends after 60 idle seconds, capped by a
--     resource monitor, so a forgotten job cannot burn the trial credits.
--   * Four roles with least privilege: the loader can only write RAW, the
--     transformer can read RAW and build models, analysts can read marts but
--     not the RESTRICTED schema, and one role may see unmasked PII.
--   * A service user that logs in with a key pair (no password to leak).
--   * Masking and row access policies used by the dbt models.
-- =============================================================================

use role accountadmin;

-- ---- compute, with a hard spending cap ---------------------------------------
create warehouse if not exists CLAIMS_WH
    warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true
    statement_timeout_in_seconds = 900
    comment = 'claims-cost-mart: all loads, dbt runs and evals';

create or replace resource monitor CLAIMS_MONITOR
    with credit_quota = 25 frequency = monthly start_timestamp = immediately
    triggers on 75 percent do notify
             on 100 percent do suspend
             on 110 percent do suspend_immediate;
alter warehouse CLAIMS_WH set resource_monitor = CLAIMS_MONITOR;

-- ---- database and schemas -----------------------------------------------------
create database if not exists CLAIMS comment = 'Synthetic healthcare claims. No real patient data.';
create schema if not exists CLAIMS.RAW        comment = 'Append-only landing tables';
create schema if not exists CLAIMS.GOVERNANCE comment = 'Masking and row access policies';
create schema if not exists CLAIMS.STAGING;
create schema if not exists CLAIMS.INTERMEDIATE;
create schema if not exists CLAIMS.SNAPSHOTS;
create schema if not exists CLAIMS.MARTS;
create schema if not exists CLAIMS.RESTRICTED comment = 'Direct identifiers. PII reader role only';
create schema if not exists CLAIMS.TEST_FAILURES;

create file format if not exists CLAIMS.RAW.PARQUET_FF type = parquet use_logical_type = true;
create stage if not exists CLAIMS.RAW.LANDING file_format = CLAIMS.RAW.PARQUET_FF;

-- ---- roles ---------------------------------------------------------------------
create role if not exists CLAIMS_LOADER       comment = 'Writes RAW only';
create role if not exists CLAIMS_TRANSFORMER  comment = 'Reads RAW, builds dbt models';
create role if not exists CLAIMS_ANALYST      comment = 'Reads marts. No RESTRICTED, PII masked';
create role if not exists CLAIMS_PII_READER   comment = 'May see unmasked identifiers';
grant role CLAIMS_LOADER      to role CLAIMS_TRANSFORMER;
grant role CLAIMS_ANALYST     to role CLAIMS_PII_READER;
grant role CLAIMS_TRANSFORMER to role sysadmin;
grant role CLAIMS_PII_READER  to role sysadmin;

grant usage on warehouse CLAIMS_WH to role CLAIMS_LOADER;
grant usage on warehouse CLAIMS_WH to role CLAIMS_ANALYST;
grant usage on database CLAIMS to role CLAIMS_LOADER;
grant usage on database CLAIMS to role CLAIMS_ANALYST;

-- loader: RAW only
grant usage, create table on schema CLAIMS.RAW to role CLAIMS_LOADER;
grant read, write on stage CLAIMS.RAW.LANDING to role CLAIMS_LOADER;
grant usage on file format CLAIMS.RAW.PARQUET_FF to role CLAIMS_LOADER;
grant select, insert, truncate on all tables in schema CLAIMS.RAW to role CLAIMS_LOADER;
grant select, insert, truncate on future tables in schema CLAIMS.RAW to role CLAIMS_LOADER;

-- transformer: owns the modelled schemas, may create CI sandbox schemas
grant create schema on database CLAIMS to role CLAIMS_TRANSFORMER;
grant all on schema CLAIMS.STAGING       to role CLAIMS_TRANSFORMER;
grant all on schema CLAIMS.INTERMEDIATE  to role CLAIMS_TRANSFORMER;
grant all on schema CLAIMS.SNAPSHOTS     to role CLAIMS_TRANSFORMER;
grant all on schema CLAIMS.MARTS         to role CLAIMS_TRANSFORMER;
grant all on schema CLAIMS.RESTRICTED    to role CLAIMS_TRANSFORMER;
grant all on schema CLAIMS.TEST_FAILURES to role CLAIMS_TRANSFORMER;
grant usage on schema CLAIMS.GOVERNANCE  to role CLAIMS_TRANSFORMER;

-- analyst: marts only
grant usage on schema CLAIMS.MARTS to role CLAIMS_ANALYST;
grant select on all tables in schema CLAIMS.MARTS to role CLAIMS_ANALYST;
grant select on future tables in schema CLAIMS.MARTS to role CLAIMS_ANALYST;

-- PII reader: also the restricted schema
grant usage on schema CLAIMS.RESTRICTED to role CLAIMS_PII_READER;
grant select on all tables in schema CLAIMS.RESTRICTED to role CLAIMS_PII_READER;
grant select on future tables in schema CLAIMS.RESTRICTED to role CLAIMS_PII_READER;

-- ---- governance policies -----------------------------------------------------------
create masking policy if not exists CLAIMS.GOVERNANCE.PII_STRING_MASK as (val string) returns string ->
    case when is_role_in_session('CLAIMS_PII_READER') then val else '***MASKED***' end;

create masking policy if not exists CLAIMS.GOVERNANCE.PII_DATE_MASK as (val date) returns date ->
    case when is_role_in_session('CLAIMS_PII_READER') then val else date_trunc('year', val)::date end;

-- which role may see which payer's claims. Roles not listed see everything
-- only if they are the transformer or PII reader.
create table if not exists CLAIMS.GOVERNANCE.PAYER_ACCESS (role_name string, payer_id string);

create row access policy if not exists CLAIMS.GOVERNANCE.PAYER_ROW_ACCESS as (payer_id string) returns boolean ->
    is_role_in_session('CLAIMS_TRANSFORMER')
    or is_role_in_session('CLAIMS_PII_READER')
    or exists (
        select 1 from CLAIMS.GOVERNANCE.PAYER_ACCESS a
        where a.payer_id = payer_id and is_role_in_session(a.role_name)
    )
    or not exists (
        -- a role with no rows in the mapping is unrestricted (plain analyst)
        select 1 from CLAIMS.GOVERNANCE.PAYER_ACCESS a where is_role_in_session(a.role_name)
    );

grant apply on masking policy CLAIMS.GOVERNANCE.PII_STRING_MASK to role CLAIMS_TRANSFORMER;
grant apply on masking policy CLAIMS.GOVERNANCE.PII_DATE_MASK   to role CLAIMS_TRANSFORMER;
grant apply on row access policy CLAIMS.GOVERNANCE.PAYER_ROW_ACCESS to role CLAIMS_TRANSFORMER;
grant select on table CLAIMS.GOVERNANCE.PAYER_ACCESS to role CLAIMS_TRANSFORMER;

-- ---- service user (key-pair auth, no password) ---------------------------------------
-- Replace <RSA_PUBLIC_KEY> with the public key body. The private key never
-- goes into this repo: it lives in GitHub Actions secrets only.
create user if not exists CLAIMS_SVC
    type = service
    default_role = CLAIMS_TRANSFORMER
    default_warehouse = CLAIMS_WH
    default_namespace = CLAIMS.RAW
    rsa_public_key = '<RSA_PUBLIC_KEY>'
    comment = 'claims-cost-mart pipeline (GitHub Actions)';
grant role CLAIMS_TRANSFORMER to user CLAIMS_SVC;

-- ---- sanity check --------------------------------------------------------------------
select current_account() as account, current_region() as region, current_version() as version;
