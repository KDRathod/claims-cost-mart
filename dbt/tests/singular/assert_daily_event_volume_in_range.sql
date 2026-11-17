{{ config(severity='warn') }}
-- Volume monitor. Compares the latest complete day of ledger events with the
-- median of the 14 days before it and reports a drop below 25% or a spike
-- above 400%. A feed that silently sends a near-empty file still passes
-- every uniqueness and not-null test, so volume needs its own check.
-- Warn only: a quiet day should page a person, not block the marts.
with daily as (
    select posted_date, count(*) as events
    from {{ ref('fct_claim_events') }}
    group by 1
),

latest as (
    select max(posted_date) as latest_day from daily
),

baseline as (
    select median(d.events) as median_events
    from daily d
    cross join latest l
    where d.posted_date <  l.latest_day
      and d.posted_date >= {{ dbt.dateadd('day', -14, 'l.latest_day') }}
)

select d.posted_date, d.events, b.median_events
from daily d
cross join latest l
cross join baseline b
where d.posted_date = l.latest_day
  and b.median_events >= 10
  and (d.events < 0.25 * b.median_events or d.events > 4 * b.median_events)
