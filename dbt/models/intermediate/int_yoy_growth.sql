-- depends_on: ref('stg_anrt__mobile')
-- YoY growth = (current quarter - same quarter last year) / same quarter last year.
-- LAG(4) over quarterly order gives the same quarter one year prior.

with market_by_quarter as (
    select
        year,
        quarter,
        sum(total_subs) as market_total_subs
    from {{ ref('stg_anrt__mobile') }}
    where total_subs is not null
    group by year, quarter
),

with_lag as (
    select
        year,
        quarter,
        market_total_subs,
        lag(market_total_subs, 4) over (
            order by year, quarter
        )                                                  as prev_year_subs
    from market_by_quarter
),

with_growth as (
    select
        year,
        quarter,
        market_total_subs,
        prev_year_subs,
        round(
            100.0 * (market_total_subs - prev_year_subs) / nullif(prev_year_subs, 0),
            2
        )                                                  as yoy_growth_pct
    from with_lag
)

select * from with_growth
