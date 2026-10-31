-- Business rule: if more than 15 % of the delivery rows are rejected, the source is broken -> fail the build.
-- dbt build then skips everything downstream: the bad batch never reaches gold (incident 3).
select q.n as quarantined, s.n as bronze_rows
from (select count(*) n from {{ ref('deliveries_quarantine') }}) q
cross join (select count(*) n from {{ ref('int_deliveries_checked') }}) s
where q.n / nullif(s.n, 0) > 0.15
