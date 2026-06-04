-- depends_on: source('bronze_anrt', 'bronze_anrt_traffic')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_traffic') }}
),

renamed as (
    select
        cast(year as integer)          as year,
        cast(quarter as varchar)       as quarter,
        cast(segment as varchar)       as segment,
        cast(voice_minutes as bigint)  as voice_minutes,
        cast(sms_count as bigint)      as sms_count
    from source
    where year is not null
      and voice_minutes is not null
)

select * from renamed
