-- One row per depot_id (latest file wins). Two depots can share a city: always group by depot_id, never by name.
select
    try_to_number(depot_id)                      as depot_id,
    initcap({{ clean_text('city') }})            as city,
    {{ clean_int('docks') }}                     as docks
from {{ source('bronze', 'depots') }}
where try_to_number(depot_id) is not null
qualify row_number() over (partition by try_to_number(depot_id) order by _loaded_at desc, _source_file desc) = 1
