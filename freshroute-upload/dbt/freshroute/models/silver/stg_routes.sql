-- One row per route_id; the latest batch wins (a route planned on day 1 can be completed on day 2).
select
    try_to_number(r.route_id)                        as route_id,
    try_to_number({{ clean_text('r.vehicle_id') }})  as vehicle_id,
    try_to_number({{ clean_text('r.depot_id') }})    as depot_id,
    {{ clean_ts('r.planned_start') }}                as planned_start,
    {{ clean_int('r.planned_stops') }}               as planned_stops,
    m.clean_value                                    as route_status
from {{ source('bronze', 'routes') }} r
left join {{ ref('category_mapping') }} m
       on m.domain = 'route_status' and m.raw_value = {{ normalise_category('r.status') }}
where try_to_number(r.route_id) is not null
qualify row_number() over (partition by try_to_number(r.route_id)
                           order by r._loaded_at desc, r._source_file desc, (m.clean_value is null)) = 1
