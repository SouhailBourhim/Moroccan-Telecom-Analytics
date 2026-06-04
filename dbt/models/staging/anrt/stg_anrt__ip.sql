-- depends_on: source('bronze_anrt', 'bronze_anrt_ip')
-- Note: IP dataset has no quarter column — annual granularity only.

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_ip') }}
),

renamed as (
    select
        cast(year as integer)           as year,
        cast(ipv4_count as bigint)      as ipv4_count,
        cast(ipv6_prefixes as bigint)   as ipv6_prefixes
    from source
    where year is not null
)

select * from renamed
