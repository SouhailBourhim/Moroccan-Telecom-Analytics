-- depends_on: source('bronze_anrt', 'bronze_anrt_arpm')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_arpm') }}
),

renamed as (
    select
        cast(year as integer)              as year,
        cast(quarter as varchar)           as quarter,
        cast(arpm as double)               as arpm,
        cast(internet_bill_avg as double)  as internet_bill_avg
    from source
    where year is not null
      and arpm is not null
)

select * from renamed
