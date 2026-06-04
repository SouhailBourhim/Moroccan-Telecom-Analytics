-- depends_on: source('bronze_anrt', 'bronze_anrt_data_links')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_data_links') }}
),

renamed as (
    select
        cast(year as integer)       as year,
        cast(quarter as varchar)    as quarter,
        cast(link_type as varchar)  as link_type,
        cast(count as bigint)       as link_count
    from source
    where year is not null
      and count is not null
)

select * from renamed
