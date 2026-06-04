-- Fail if any gold mart table has zero rows (guards against silent empty loads).
-- Returns one row per empty table; test passes when this query returns 0 rows.

with mart_counts as (
    select 'mart_market_overview' as mart, count(*) as row_count from {{ ref('mart_market_overview') }}
    union all
    select 'mart_operator_perf',          count(*) from {{ ref('mart_operator_perf') }}
    union all
    select 'mart_qos_scorecard',          count(*) from {{ ref('mart_qos_scorecard') }}
    union all
    select 'mart_internet_evol',          count(*) from {{ ref('mart_internet_evol') }}
    union all
    select 'mart_benchmarks',             count(*) from {{ ref('mart_benchmarks') }}
)

select mart, row_count
from mart_counts
where row_count = 0
