insert into analysis.tmp_rfm_monetary_value (user_id, monetary_value)
with monetary as (select u.id, u.name, u.login , sum (coalesce(ord.cost,0)) order_sum, ord."key" 
from users u 
left join (select * from orders o left join orderstatuses o2 on o.status = o2.id 
where o2.key = 'Closed' and order_ts>'2022-01-01 00:00:00.000') as ord on u.id =ord.user_id 
group by u.id, u.name, u.login, ord."key" 
order by u.id
)
select id
, ntile (5) OVER (ORDER BY order_sum) monetary
from monetary m
order by id