-- depends_on: source('bronze_anrt', 'bronze_anrt_usage_avg')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_usage_avg') }}
),

renamed as (
    select
        cast(year as integer)           as year,
        cast(quarter as varchar)        as quarter,
        cast(mobile_minutes as double)  as mobile_minutes,
        cast(fixed_minutes as double)   as fixed_minutes
    from source
    where year is not null
)

select * from renamed
