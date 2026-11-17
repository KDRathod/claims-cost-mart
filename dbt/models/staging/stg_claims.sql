with source as (
    select * from {{ source('raw', 'claims') }}
),

-- A claim header can be delivered more than once. Keep the latest delivery.
deduped as (
    select *
    from source
    qualify row_number() over (partition by claim_id order by _loaded_at desc, _source_file desc) = 1
)

select
    cast(claim_id as varchar)               as claim_id,
    cast(member_id as varchar)              as member_id,
    cast(provider_id as varchar)            as provider_id,
    cast(payer_id as varchar)               as payer_id,
    cast(service_date as date)              as service_date,
    {{ month_start('service_date') }}       as service_month,
    cast(submitted_date as date)            as submitted_date,
    lower(encounter_class)                  as encounter_class,
    {{ service_category('lower(encounter_class)') }} as service_category,
    lower(claim_type)                       as claim_type,
    cast(primary_diagnosis_code as varchar) as primary_diagnosis_code,
    encounter_reason,
    {{ money('billed_amount') }}            as billed_amount,
    _source_file,
    _loaded_at
from deduped
