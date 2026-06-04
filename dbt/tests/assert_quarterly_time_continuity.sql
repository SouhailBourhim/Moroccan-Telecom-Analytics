-- Fail if there are gaps in the quarterly time series for mobile data.
-- A gap is a (year, quarter) combination that is missing between the
-- first and last observed period. Returns one row per gap found.

with periods as (
    select distinct year, quarter from {{ ref('stg_anrt__mobile') }}
),

all_quarters as (
    -- dim_period.quarter is an integer (1-4); convert to 'Q1' format to match staging
    select year, 'Q' || cast(quarter as varchar) as quarter
    from {{ ref('dim_period') }}
    where year between (select min(year) from periods)
      and (select max(year) from periods)
),

gaps as (
    select a.year, a.quarter
    from all_quarters a
    left join periods p on a.year = p.year and a.quarter = p.quarter
    where p.year is null
)

select * from gaps
