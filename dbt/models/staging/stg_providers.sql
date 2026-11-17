select
    cast(provider_id as varchar)   as provider_id,
    provider_name,
    organization_name,
    city,
    upper(state)                   as state,
    cast(zip_code as varchar)      as zip_code,
    lower(network_status)          as network_status,
    lower(network_tier)            as network_tier,
    cast(extract_date as date)     as extract_date
from {{ source('raw', 'providers') }}
