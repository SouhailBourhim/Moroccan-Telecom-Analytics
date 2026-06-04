-- depends_on: source('bronze_anrt', 'bronze_anrt_domains')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_domains') }}
),

renamed as (
    select
        cast(year as integer)           as year,
        cast(quarter as varchar)        as quarter,
        cast(active_domains as bigint)  as active_domains
    from source
    where year is not null
      and active_domains is not null
)

select * from renamed
