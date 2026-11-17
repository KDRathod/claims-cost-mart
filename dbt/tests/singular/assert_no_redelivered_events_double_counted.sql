-- The feed redelivers rows. The raw table keeps every copy (by design), the
-- fact table must keep exactly one. Fails if a duplicate slips through.
select event_id, count(*) as copies
from {{ ref('fct_claim_events') }}
group by event_id
having count(*) > 1
