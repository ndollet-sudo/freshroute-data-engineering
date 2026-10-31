-- UC1 - Depot manager: which depot misses its delivery promise?
-- Grain: business_date x depot_id.  OTIF = delivered on time / all deliveries due that day.
select
    business_date,
    depot_id,
    any_value(city)                                       as city,
    count(*)                                              as deliveries_due,
    count_if(is_delivered)                                as delivered,
    count_if(is_on_time)                                  as delivered_on_time,
    round(delivered / deliveries_due, 4)                  as success_rate,
    round(delivered_on_time / deliveries_due, 4)          as otif_rate,
    count_if(outcome = 'failed_not_home')                 as failed_not_home,
    count_if(outcome = 'refused')                         as refused,
    count_if(outcome = 'damaged')                         as damaged,
    otif_rate >= 0.85                                     as meets_target_85
from {{ ref('fct_deliveries') }}
group by business_date, depot_id
