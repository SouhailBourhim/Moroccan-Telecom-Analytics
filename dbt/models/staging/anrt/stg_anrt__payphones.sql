-- depends_on: source('bronze_anrt', 'bronze_anrt_payphones')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_payphones') }}
),

renamed as (
    select
        cast(year as integer)           as year,
        cast(quarter as varchar)        as quarter,
        cast(total_payphones as bigint) as total_payphones
    from source
    where year is not null
      and total_payphones is not null
)

select * from renamed
