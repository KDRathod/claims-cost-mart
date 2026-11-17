{{ config(severity='warn') }}
-- Claims whose service date falls outside every coverage span for that
-- member and payer. A handful exist in the source. Warn, do not block:
-- finance should review them, but the pipeline should still publish.
select c.claim_id, c.member_id, c.payer_id, c.service_date
from {{ ref('stg_claims') }} c
left join {{ ref('stg_enrollment') }} e
    on  c.member_id = e.member_id
    and c.payer_id  = e.payer_id
    and c.service_date between e.coverage_start_date and e.coverage_end_date
where e.member_id is null
