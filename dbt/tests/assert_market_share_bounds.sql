-- Fail if market share per period falls outside [95, 105].
-- A small tolerance around 100% accounts for rounding in source data.
-- Returns one row per period where the total deviates from 100%.

with totals as (
    select
        year,
        quarter,
        sum(market_share_pct) as total_share
    from {{ ref('mart_operator_perf') }}
    where market_share_pct is not null
    group by year, quarter
)

select year, quarter, total_share
from totals
where total_share < 95 or total_share > 105
