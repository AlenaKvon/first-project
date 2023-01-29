insert into analysis.tmp_rfm_frequency (user_id, frequency)
with frequency as (select u.id, u.name, u.login , count (ord.order_ts) order_count, ord."key" 
from users u 
left join (select * from orders o left join orderstatuses o2 on o.status = o2.id 
where o2.key = 'Closed' and order_ts>'2022-01-01 00:00:00.000') as ord on u.id =ord.user_id 
group by u.id, u.name, u.login, ord."key" 
order by u.id
)
select id
, ntile (5) OVER (ORDER BY order_count) frequency
from frequency f
order by id