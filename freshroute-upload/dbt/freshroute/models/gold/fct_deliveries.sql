-- Grain: one row per delivery. Business day = the day the delivery was PROMISED, in London time.
select
    d.delivery_id,
    d.route_id,
    r.vehicle_id,
    r.depot_id,
    dp.city,
    convert_timezone('Europe/London', d.promised_by)::date          as business_date,
    d.promised_by,
    d.delivered_at,
    d.outcome,
    d.outcome = 'delivered'                                         as is_delivered,
    d.outcome = 'delivered' and d.delivered_at <= d.promised_by     as is_on_time,
    iff(d.outcome = 'delivered',
        greatest(datediff('minute', d.promised_by, d.delivered_at), 0), null) as late_minutes,
    d.delivery_fee_gbp,
    d.parcel_kg,
    coalesce(w.rain_category, 'unknown')                            as rain_category,
    w.precipitation_mm,
    h.holiday_date is not null                                      as is_bank_holiday
from {{ ref('silver_deliveries') }} d
join {{ ref('stg_routes') }} r            on r.route_id = d.route_id
left join {{ ref('stg_depots') }} dp      on dp.depot_id = r.depot_id
left join {{ ref('depot_cities') }} c     on c.city = dp.city
left join {{ ref('stg_weather_daily') }} w
       on w.city = dp.city and w.weather_date = convert_timezone('Europe/London', d.promised_by)::date
left join {{ ref('stg_bank_holidays') }} h
       on h.holiday_division = c.holiday_division and h.holiday_date = convert_timezone('Europe/London', d.promised_by)::date
