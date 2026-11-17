select
    {{ dbt_utils.generate_surrogate_key(['member_id', 'payer_id', 'coverage_start_date']) }} as enrollment_span_id,
    cast(member_id as varchar)            as member_id,
    cast(payer_id as varchar)             as payer_id,
    cast(coverage_start_date as date)     as coverage_start_date,
    cast(coverage_end_date as date)       as coverage_end_date,
    lower(subscriber_relationship)        as subscriber_relationship
from {{ source('raw', 'enrollment') }}
-- a span that ends before it starts is unusable; the test on the source reports it
where cast(coverage_end_date as date) >= cast(coverage_start_date as date)
