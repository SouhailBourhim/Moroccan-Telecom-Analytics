-- Fails if any (year, quarter) period has operator market shares that don't sum to ~100%
select year, quarter, sum(market_share_pct) as total_share
from {{ ref('mart_operator_perf') }}
group by year, quarter
having sum(market_share_pct) not between 99 and 101
