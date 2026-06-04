-- depends_on: source('bronze_itu', 'bronze_itu_morocco')

with source as (
    select * from {{ source('bronze_itu', 'bronze_itu_morocco') }}
),

renamed as (
    select
        cast(year as integer)            as year,
        cast(indicator_code as varchar)  as indicator_code,
        cast(indicator_name as varchar)  as indicator_name,
        cast(value as double)            as value,
        cast(unit as varchar)            as unit,
        cast(data_source as varchar)     as data_source
    from source
    where year is not null
      and value is not null
)

select * from renamed
