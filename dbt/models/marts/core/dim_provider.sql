{#
  Type 2 provider dimension built from the snapshot. One row per provider per
  version. Join facts with:  service_date >= valid_from and service_date < valid_to
#}

with snapshot as (
    select * from {{ ref('snap_providers') }}
),

versions as (

    select
        dbt_scd_id                                   as provider_key,
        provider_id,
        provider_name,
        organization_name,
        city,
        state,
        zip_code,
        network_status,
        network_tier,
        cast(dbt_valid_from as date)                 as observed_from,
        cast(dbt_valid_to as date)                   as observed_to,
        row_number() over (partition by provider_id order by dbt_valid_from) as version_number
    from snapshot

)

select
    provider_key,
    provider_id,
    provider_name,
    organization_name,
    city,
    state,
    zip_code,
    network_status,
    network_tier,
    version_number,
    -- The first version we ever saw is assumed to hold back to the beginning.
    -- Without this, claims older than our first snapshot would match nothing.
    case when version_number = 1 then cast('1900-01-01' as date) else observed_from end as valid_from,
    coalesce(observed_to, cast('9999-12-31' as date))                                   as valid_to,
    observed_to is null                                                                 as is_current
from versions
