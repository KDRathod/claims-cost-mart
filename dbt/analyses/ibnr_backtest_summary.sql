-- How wrong is "paid so far" versus the completion-adjusted estimate,
-- by months of run-out? Mean absolute percentage error across service months.
select
    months_of_runout,
    count(*)                                         as service_months_tested,
    round(avg(abs(naive_error_pct)) * 100, 1)        as naive_mape_pct,
    round(avg(abs(estimate_error_pct)) * 100, 1)     as estimate_mape_pct,
    round(max(abs(estimate_error_pct)) * 100, 1)     as estimate_worst_month_pct
from {{ ref('mart_ibnr_backtest') }}
group by 1
order by 1
