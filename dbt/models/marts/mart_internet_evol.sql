-- depends_on: ref('stg_anrt__internet'), ref('stg_anrt__bandwidth'),
--             ref('int_penetration_rates')
-- Subscriber counts are in thousands.

with internet_by_tech as (
    select
        year,
        quarter,
        sum(case when technology = 'ADSL'   then subscribers else 0 end) as adsl_subs,
        sum(case when technology = 'FTTH'   then subscribers else 0 end) as ftth_subs,
        sum(case when technology = 'Mobile' then subscribers else 0 end) as mobile_bb_subs,
        sum(case when technology = 'Leased' then subscribers else 0 end) as leased_subs,
        sum(case when technology = 'Other'  then subscribers else 0 end) as other_subs,
        sum(subscribers)                                                  as total_internet_subs
    from {{ ref('stg_anrt__internet') }}
    where technology != 'Total'
    group by year, quarter
),

bandwidth as (
    select year, capacity_gbps as intl_bandwidth_gbps
    from {{ ref('stg_anrt__bandwidth') }}
),

penetration as (
    select year, mobile_penetration_per_100
    from {{ ref('int_penetration_rates') }}
)

select
    it.year,
    it.quarter,
    it.adsl_subs,
    it.ftth_subs,
    it.mobile_bb_subs,
    it.leased_subs,
    it.other_subs,
    it.total_internet_subs,
    -- Penetration is annual (Q4 snapshot); show for all quarters of that year
    p.mobile_penetration_per_100,
    b.intl_bandwidth_gbps
from internet_by_tech it
left join penetration p using (year)
left join bandwidth   b using (year)
order by it.year, it.quarter
