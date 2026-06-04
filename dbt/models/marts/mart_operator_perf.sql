-- depends_on: ref('int_market_share'), ref('stg_anrt__arpm'),
--             ref('stg_anrt__traffic'), ref('stg_anrt__complaints')

with market_share as (
    select * from {{ ref('int_market_share') }}
),

arpm as (
    select year, quarter, arpm, internet_bill_avg
    from {{ ref('stg_anrt__arpm') }}
),

mobile_traffic as (
    select year, quarter, voice_minutes as mobile_voice_minutes, sms_count
    from {{ ref('stg_anrt__traffic') }}
    where segment = 'mobile'
),

-- Complaints have no operator breakdown; aggregate to period level
complaints as (
    select
        year,
        quarter,
        sum(complaint_count) as total_complaints
    from {{ ref('stg_anrt__complaints') }}
    group by year, quarter
)

select
    ms.year,
    ms.quarter,
    ms.operator,
    ms.operator_subs,
    ms.market_total_subs,
    ms.market_share_pct,
    a.arpm,
    a.internet_bill_avg,
    mt.mobile_voice_minutes,
    mt.sms_count,
    c.total_complaints
from market_share ms
left join arpm           a  using (year, quarter)
left join mobile_traffic mt using (year, quarter)
left join complaints     c  using (year, quarter)
order by ms.year, ms.quarter, ms.operator
