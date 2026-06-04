-- depends_on: ref('stg_anrt__qos')

with qos as (
    select * from {{ ref('stg_anrt__qos') }}
)

select
    year,
    quarter,
    operator,
    indicator,
    value,
    unit
from qos
order by year, quarter, indicator
