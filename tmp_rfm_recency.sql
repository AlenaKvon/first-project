insert into analysis.tmp_rfm_recency (user_id, recency)
with recency as (select u.id, u.name, u.login , max(coalesce (order_ts, '1900-01-01 10:10:10.000')) order_max, ord."key" 
from users u 
left join (select * from orders o left join orderstatuses o2 on o.status = o2.id 
where o2.key = 'Closed' and order_ts>'2022-01-01 00:00:00.000') as ord on u.id =ord.user_id 
group by u.id, u.name, u.login, ord."key" 
order by u.id
)
select id
, ntile (5) OVER (ORDER BY order_max) recency
from recency r
order by id