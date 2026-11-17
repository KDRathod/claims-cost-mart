{#
  Typed, renamed, NOT deduplicated. Deduplication happens in fct_claim_events
  so that it runs incrementally instead of over the whole history on every
  query against this view.
#}
select
    cast(event_id as varchar)                         as event_id,
    cast(claim_id as varchar)                         as claim_id,
    upper(event_type)                                 as event_type,
    cast(event_ts as timestamp)                       as event_at,
    cast(posted_date as date)                         as posted_date,
    {{ money('allowed_amount_delta') }}               as allowed_amount_delta,
    {{ money('paid_amount_delta') }}                  as paid_amount_delta,
    {{ money('member_responsibility_delta') }}        as member_responsibility_delta,
    reason_code,
    reason_description,
    -- column added to the feed in July 2025: absent from older tables, null in older rows
    coalesce({{ column_or_null(source('raw', 'claim_events'), 'adjudication_system') }}, 'CORE') as adjudication_system,
    _source_file,
    _loaded_at
from {{ source('raw', 'claim_events') }}
