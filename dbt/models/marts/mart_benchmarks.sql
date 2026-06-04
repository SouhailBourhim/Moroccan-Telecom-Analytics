-- depends_on: ref('stg_itu__morocco')
-- One row per year; pivots ITU indicators into columns for Metabase time-series charts.

with itu as (
    select * from {{ ref('stg_itu__morocco') }}
)

select
    year,
    max(case when indicator_code = 'i271'   then value end)  as mobile_subs_total,
    max(case when indicator_code = 'i271p'  then value end)  as prepaid_mobile_subs,
    max(case when indicator_code = 'i271mw' then value end)  as active_mobile_bb_subs,
    max(case when indicator_code = 'i992b'  then value end)  as fixed_bb_subs,
    max(case when indicator_code = 'i112'   then value end)  as fixed_subs_total,
    max(case when indicator_code = 'i99H'   then value end)  as internet_users_pct,
    max(case when indicator_code = 'i4214'  then value end)  as intl_bandwidth_mbps,
    max(case when indicator_code = 'i741$'  then value end)  as mobile_revenue_musd
from itu
group by year
order by year
