-- depends_on: source('bronze_anrt', 'bronze_anrt_portability')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_portability') }}
),

renamed as (
    select
        cast(year as integer)        as year,
        cast(quarter as varchar)     as quarter,
        cast(segment as varchar)     as segment,
        cast(ported_numbers as bigint) as ported_numbers
    from source
    where year is not null
      and ported_numbers is not null
)

select * from renamed
