
CREATE OR REPLACE VIEW analysis.Orders AS 
select 
o.order_id, 
o.order_ts, 
o.user_id, 
o.bonus_payment, 
o.payment, 
o."cost", 
o.bonus_grant, 
max (o2.status_id) status
from production.orders o left join production.orderstatuslog o2  on o.order_id =o2.order_id 
group by o.order_id, o.order_ts, o.user_id, o.bonus_payment, o.payment, o.cost, o.bonus_grant 