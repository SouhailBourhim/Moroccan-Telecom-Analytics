-- depends_on: ref('stg_anrt__mobile')

with mobile as (
    select year, quarter, operator, total_subs
    from {{ ref('stg_anrt__mobile') }}
    where total_subs is not null
),

period_totals as (
    select
        year,
        quarter,
        sum(total_subs) as market_total_subs
    from mobile
    group by year, quarter
),

with_share as (
    select
        m.year,
        m.quarter,
        m.operator,
        m.total_subs                                                           as operator_subs,
        pt.market_total_subs,
        round(100.0 * m.total_subs / nullif(pt.market_total_subs, 0), 2)      as market_share_pct
    from mobile m
    join period_totals pt using (year, quarter)
)

select * from with_share
