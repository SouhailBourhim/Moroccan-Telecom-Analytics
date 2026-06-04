-- depends_on: source('bronze_anrt', 'bronze_anrt_qos')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_qos') }}
),

renamed as (
    select
        cast(year as integer)      as year,
        cast(quarter as varchar)   as quarter,
        cast(operator as varchar)  as operator,
        cast(indicator as varchar) as indicator,
        cast(value as double)      as value,
        cast(unit as varchar)      as unit
    from source
    where year is not null
      and value is not null
)

select * from renamed
