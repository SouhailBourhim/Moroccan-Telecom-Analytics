-- depends_on: source('bronze_anrt', 'bronze_anrt_mobile')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_mobile') }}
),

renamed as (
    select
        cast(year as integer)         as year,
        cast(quarter as varchar)      as quarter,
        cast(operator as varchar)     as operator,
        cast(total_subs as bigint)    as total_subs,
        cast(prepaid_subs as bigint)  as prepaid_subs,
        cast(postpaid_subs as bigint) as postpaid_subs
    from source
    where year is not null
      and operator is not null
      and operator != 'Total'
)

select * from renamed
