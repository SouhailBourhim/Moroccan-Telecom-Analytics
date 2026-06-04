-- depends_on: ref('stg_anrt__mobile'), ref('stg_anrt__internet'),
--             ref('stg_anrt__fixed'), ref('stg_anrt__traffic'),
--             ref('stg_anrt__arpm'), ref('int_yoy_growth')
-- Note: subscriber counts are in thousands; voice_minutes in millions of minutes.

with mobile as (
    select
        year,
        quarter,
        sum(total_subs) as mobile_total_subs
    from {{ ref('stg_anrt__mobile') }}
    group by year, quarter
),

internet as (
    select
        year,
        quarter,
        sum(subscribers) as internet_total_subs
    from {{ ref('stg_anrt__internet') }}
    where technology != 'Total'
    group by year, quarter
),

fixed as (
    select year, quarter, total_subs as fixed_total_subs
    from {{ ref('stg_anrt__fixed') }}
),

traffic as (
    select
        year,
        quarter,
        sum(voice_minutes) as total_voice_minutes
    from {{ ref('stg_anrt__traffic') }}
    group by year, quarter
),

arpm as (
    select year, quarter, arpm, internet_bill_avg
    from {{ ref('stg_anrt__arpm') }}
),

growth as (
    select year, quarter, yoy_growth_pct as mobile_yoy_growth_pct
    from {{ ref('int_yoy_growth') }}
)

select
    m.year,
    m.quarter,
    m.mobile_total_subs,
    i.internet_total_subs,
    f.fixed_total_subs,
    t.total_voice_minutes,
    a.arpm,
    a.internet_bill_avg,
    g.mobile_yoy_growth_pct
from mobile m
left join internet  i using (year, quarter)
left join fixed     f using (year, quarter)
left join traffic   t using (year, quarter)
left join arpm      a using (year, quarter)
left join growth    g using (year, quarter)
order by m.year, m.quarter
