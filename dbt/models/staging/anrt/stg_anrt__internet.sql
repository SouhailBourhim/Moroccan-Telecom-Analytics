-- depends_on: source('bronze_anrt', 'bronze_anrt_internet')

with source as (
    select * from {{ source('bronze_anrt', 'bronze_anrt_internet') }}
),

renamed as (
    select
        cast(year as integer)       as year,
        cast(quarter as varchar)    as quarter,
        cast(technology as varchar) as technology,
        -- FTTH is reported in absolute units in the source; convert to thousands
        -- to match ADSL, Mobile, Leased, Other which are all in thousands.
        case
            when technology = 'FTTH'
                then cast(cast(subscribers as bigint) / 1000.0 as bigint)
            else cast(subscribers as bigint)
        end                         as subscribers
    from source
    where year is not null
      and subscribers is not null
      and cast(subscribers as bigint) > 0
)

select * from renamed
