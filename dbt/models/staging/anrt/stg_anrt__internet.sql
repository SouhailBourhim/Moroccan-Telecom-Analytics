-- depends_on: source('bronze_anrt', 'bronze_anrt_internet')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_internet') }}
),

renamed as (
    select
        cast(year as integer)       as year,
        cast(quarter as varchar)    as quarter,
        cast(technology as varchar) as technology,
        cast(subscribers as bigint) as subscribers
    from source
    where year is not null
      and subscribers is not null
      and cast(subscribers as bigint) > 0
)

select * from renamed
