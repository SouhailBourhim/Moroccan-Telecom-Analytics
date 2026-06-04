-- depends_on: source('bronze_anrt', 'bronze_anrt_bandwidth')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_bandwidth') }}
),

renamed as (
    select
        cast(year as integer)           as year,
        cast(capacity_gbps as double)   as capacity_gbps
    from source
    where year is not null
      and capacity_gbps is not null
)

select * from renamed
