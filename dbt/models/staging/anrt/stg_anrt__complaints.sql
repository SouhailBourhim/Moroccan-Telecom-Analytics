-- depends_on: source('bronze_anrt', 'bronze_anrt_complaints')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_complaints') }}
),

renamed as (
    select
        cast(year as integer)           as year,
        cast(quarter as varchar)        as quarter,
        cast(operator as varchar)       as operator,
        cast(complaint_type as varchar) as complaint_type,
        cast(count as integer)          as complaint_count
    from source
    where year is not null
      and count is not null
)

select * from renamed
