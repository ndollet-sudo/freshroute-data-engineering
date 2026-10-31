-- Reconciliation: the billed fees in gold must equal the delivered fees in silver (no row lost or doubled).
select s.total as silver_gbp, g.total as gold_gbp
from (select sum(iff(outcome = 'delivered', delivery_fee_gbp, 0)) total from {{ ref('silver_deliveries') }}) s
cross join (select sum(delivery_fee_gbp) total from {{ ref('gold_revenue_weekly') }}) g
where abs(coalesce(s.total, 0) - coalesce(g.total, 0)) > 0.01
