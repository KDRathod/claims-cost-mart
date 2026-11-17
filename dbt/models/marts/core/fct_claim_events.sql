{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

{#
  Deduplicated claim ledger. One row per event, ever.

  Incremental logic, and why:
  * The high-water mark is _loaded_at (when WE received the row), not
    posted_date or event_at (when it happened). A file that shows up three
    weeks late still has a fresh _loaded_at, so it is picked up. Filtering on
    the business date would silently skip it.
  * We re-read a few days behind the mark. If a run dies halfway, the next
    run covers the gap, and the merge on event_id makes the overlap harmless.
  * Redelivered rows share an event_id. We keep the first copy we received.
#}

with new_events as (

    select *
    from {{ ref('stg_claim_events') }}
    {% if is_incremental() %}
    where _loaded_at > (
        select {{ dbt.dateadd('day', -var('incremental_lookback_days'), 'max(_loaded_at)') }}
        from {{ this }}
    )
    {% endif %}

),

deduped as (

    select *
    from new_events
    qualify row_number() over (partition by event_id order by _loaded_at, _source_file) = 1

),

claims as (
    select claim_id, member_id, payer_id, provider_id, service_date, service_month, service_category
    from {{ ref('stg_claims') }}
)

select
    e.event_id,
    e.claim_id,
    e.event_type,
    e.event_at,
    e.posted_date,
    {{ month_start('e.posted_date') }}                            as posted_month,
    c.member_id,
    c.payer_id,
    c.provider_id,
    c.service_date,
    c.service_month,
    c.service_category,
    -- months of run-out between the service and this posting
    {{ months_between('c.service_month', month_start('e.posted_date')) }} as lag_months,
    e.allowed_amount_delta,
    e.paid_amount_delta,
    e.member_responsibility_delta,
    e.reason_code,
    e.reason_description,
    e.adjudication_system,
    c.claim_id is null                                            as is_orphan_event,
    e._source_file,
    e._loaded_at
from deduped e
left join claims c on e.claim_id = c.claim_id
