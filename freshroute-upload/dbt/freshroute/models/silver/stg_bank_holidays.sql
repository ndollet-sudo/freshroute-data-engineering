select distinct
    b.source_key                       as holiday_division,
    e.value:date::date                 as holiday_date,
    e.value:title::string              as holiday_name
from {{ source('bronze', 'bank_holidays_raw') }} b,
     lateral flatten(input => b.payload:events) e
