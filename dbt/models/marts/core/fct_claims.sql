{{
    config(
        materialized='incremental',
        unique_key='claim_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        cluster_by=['service_month'],
        post_hook="{{ apply_row_access_policy('payer_id') }}" if target.type == 'snowflake' and env_var('APPLY_GOVERNANCE', 'false') == 'true' else ''
    )
}}

{#
  One row per claim with its current financial state.

  A claim's numbers change for months after the service (adjustments,
  reversals, appeals). On each run we rebuild only the claims that received a
  new event since the last run and merge them in by claim_id.
#}

with changed_claims as (

    -- the filter is on _loaded_at so the warehouse can prune to new rows only
    select distinct claim_id
    from {{ ref('fct_claim_events') }}
    {% if is_incremental() %}
    where _loaded_at > (select max(last_loaded_at) from {{ this }})
    {% endif %}

),

events as (

    -- every event for a changed claim, not just the new ones
    select e.*
    from {{ ref('fct_claim_events') }} e
    inner join changed_claims c on e.claim_id = c.claim_id

),

state as (
    {{ claim_state_rollup('events') }}
),

claims as (
    select * from {{ ref('stg_claims') }}
),

providers as (
    select * from {{ ref('dim_provider') }}
),

line_counts as (
    select claim_id, count(*) as line_count
    from {{ ref('stg_claim_lines') }}
    group by claim_id
)

select
    c.claim_id,
    c.member_id,
    c.payer_id,
    c.provider_id,
    p.provider_key,
    coalesce(p.network_status, 'unknown')                             as provider_network_status_at_service,
    c.service_date,
    c.service_month,
    c.encounter_class,
    c.service_category,
    c.claim_type,
    c.primary_diagnosis_code,
    coalesce(l.line_count, 0)                                         as line_count,
    c.billed_amount,
    s.allowed_amount,
    s.paid_amount,
    s.member_responsibility_amount,
    s.claim_status,
    s.denial_reason_code,
    s.was_ever_denied,
    s.was_adjusted,
    s.was_reversed,
    s.was_overturned_on_appeal,
    s.submitted_date,
    s.first_decision_date,
    s.first_paid_date,
    {{ dbt.datediff('c.service_date', 's.submitted_date', 'day') }}    as days_service_to_submission,
    {{ dbt.datediff('s.submitted_date', 's.first_decision_date', 'day') }} as days_submission_to_decision,
    s.event_count,
    s.last_posted_date,
    s.last_loaded_at
from state s
inner join claims c on s.claim_id = c.claim_id
left join line_counts l on c.claim_id = l.claim_id
-- point-in-time join: the provider's network status ON THE SERVICE DATE,
-- not what it is today
left join providers p
    on c.provider_id = p.provider_id
   and c.service_date >= p.valid_from
   and c.service_date <  p.valid_to
