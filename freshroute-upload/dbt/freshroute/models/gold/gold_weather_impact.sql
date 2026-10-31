-- UC2 - Operations director: do we need more drivers / tighter slots on rainy days?
-- Grain: depot_id x rain_category. Days without weather data are excluded.
select
    depot_id,
    any_value(city)                                       as city,
    rain_category,
    count(*)                                              as deliveries_due,
    round(count_if(is_delivered) / count(*), 4)           as success_rate,
    round(count_if(is_on_time) / count(*), 4)             as otif_rate,
    round(avg(late_minutes), 1)                           as avg_late_minutes
from {{ ref('fct_deliveries') }}
where rain_category <> 'unknown'
group by depot_id, rain_category
