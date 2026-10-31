-- payload.rates = {"2026-09-01": {"EUR": 1.18}, ...}: one row per ECB business day
select
    r.key::date              as rate_date,
    r.value:EUR::float       as gbp_to_eur
from {{ source('bronze', 'fx_raw') }} f,
     lateral flatten(input => f.payload:rates) r
qualify row_number() over (partition by rate_date order by f._loaded_at desc) = 1
