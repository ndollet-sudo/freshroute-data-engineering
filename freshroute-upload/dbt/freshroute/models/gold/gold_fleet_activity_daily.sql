-- Fleet manager: is each van actually used? Built from the GPS stream.
-- Grain: business_date x vehicle_id. moving_share = share of pings with speed > 0.
select
    convert_timezone('Europe/London', g.ping_ts)::date        as business_date,
    g.vehicle_id,
    v.depot_id,
    v.vehicle_type,
    count(*)                                                  as pings,
    round(count_if(g.speed_kmh > 0) / count(*), 4)            as moving_share,
    round(avg(iff(g.speed_kmh > 0, g.speed_kmh, null)), 1)    as avg_moving_speed_kmh,
    max(g.speed_kmh)                                          as max_speed_kmh
from {{ ref('silver_gps_pings') }} g
left join {{ ref('stg_vehicles') }} v on v.vehicle_id = g.vehicle_id
group by 1, 2, 3, 4
