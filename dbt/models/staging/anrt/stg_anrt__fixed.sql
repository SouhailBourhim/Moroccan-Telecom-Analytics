-- depends_on: source('bronze_anrt', 'bronze_anrt_fixed')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_fixed') }}
),

renamed as (
    select
        cast(year as integer)      as year,
        cast(quarter as varchar)   as quarter,
        cast(total_subs as bigint) as total_subs
    from source
    where year is not null
      and total_subs is not null
)

select * from renamed
