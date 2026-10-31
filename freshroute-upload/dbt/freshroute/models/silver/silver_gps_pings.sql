-- GPS pings: the JSON payload is sometimes 'NULL', broken, or uses LAT instead of lat -> parse defensively.
with parsed as (
    select
        try_to_number(ping_id)                               as ping_id,
        try_to_number({{ clean_text('vehicle_id') }})        as vehicle_id,
        {{ clean_ts('ping_ts') }}                            as ping_ts,
        try_parse_json({{ clean_text('payload') }})          as p,
        _batch,
        _loaded_at
    from {{ source('bronze', 'gps_pings') }}
)

select
    ping_id,
    vehicle_id,
    ping_ts,
    coalesce(p:lat, p:LAT)::float                            as latitude,
    coalesce(p:lon, p:LON)::float                            as longitude,
    try_to_number(coalesce(p:speed_kmh, p:SPEED_KMH)::string) as speed_kmh,
    lower(p:engine::string)                                  as engine_status,
    _batch
from parsed
where ping_id is not null and ping_ts is not null
  and coalesce(p:lat, p:LAT) is not null and coalesce(p:lon, p:LON) is not null
qualify row_number() over (partition by ping_id order by _batch desc, _loaded_at desc, ping_ts) = 1
