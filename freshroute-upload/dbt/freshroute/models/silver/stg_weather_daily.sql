-- Open-Meteo returns parallel arrays (daily.time[i], daily.precipitation_sum[i]): one row per city and day.
select
    w.source_key                                             as city,
    t.value::date                                            as weather_date,
    w.payload:daily:precipitation_sum[t.index]::float        as precipitation_mm,
    w.payload:daily:temperature_2m_max[t.index]::float       as temp_max_c,
    w.payload:daily:wind_speed_10m_max[t.index]::float       as wind_max_kmh,
    case
        when precipitation_mm is null then 'unknown'
        when precipitation_mm < 1     then 'dry'
        when precipitation_mm < 10    then 'light_rain'
        else 'heavy_rain'
    end                                                      as rain_category
from {{ source('bronze', 'weather_raw') }} w,
     lateral flatten(input => w.payload:daily:time) t
