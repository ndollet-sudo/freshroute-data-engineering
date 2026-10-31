select
    try_to_number(v.vehicle_id)                  as vehicle_id,
    upper({{ clean_text('v.plate') }})           as plate,
    m.clean_value                                as vehicle_type,
    try_to_number({{ clean_text('v.depot_id') }}) as depot_id,
    {{ clean_int('v.capacity_kg') }}             as capacity_kg
from {{ source('bronze', 'vehicles') }} v
left join {{ ref('category_mapping') }} m
       on m.domain = 'vehicle_type' and m.raw_value = {{ normalise_category('v.vehicle_type') }}
where try_to_number(v.vehicle_id) is not null
qualify row_number() over (partition by try_to_number(v.vehicle_id) order by v._loaded_at desc, v._source_file desc) = 1
