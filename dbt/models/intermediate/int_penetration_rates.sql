-- depends_on: ref('stg_anrt__mobile'), ref('dim_population')
-- Computes annual mobile penetration rate (Q4 end-of-year snapshot).
-- total_subs is stored in thousands; multiply by 1000 before dividing by population.

with mobile_q4 as (
    select
        year,
        sum(total_subs) as market_total_subs_thousands
    from {{ ref('stg_anrt__mobile') }}
    where quarter = 'Q4'
      and total_subs is not null
    group by year
),

population as (
    select year, population
    from {{ ref('dim_population') }}
),

with_rates as (
    select
        m.year,
        m.market_total_subs_thousands,
        p.population,
        round(
            100.0 * (m.market_total_subs_thousands * 1000.0) / nullif(p.population, 0),
            1
        )                                                  as mobile_penetration_per_100
    from mobile_q4 m
    left join population p using (year)
)

select * from with_rates
