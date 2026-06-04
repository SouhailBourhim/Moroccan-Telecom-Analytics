-- depends_on: source('bronze_anrt', 'bronze_anrt_tic_survey')
-- Note: TIC survey is long-format (one row per indicator) with no quarter.

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_tic_survey') }}
),

renamed as (
    select
        cast(year as integer)       as year,
        cast(indicator as varchar)  as indicator,
        cast(value as double)       as value,
        cast(unit as varchar)       as unit
    from source
    where year is not null
      and indicator is not null
      and value is not null
)

select * from renamed
