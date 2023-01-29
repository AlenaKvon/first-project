insert into analysis.dm_rfm_segments (user_id, recency, frequency, monetary_value)
select r.user_id, r.recency, f.frequency , m.monetary_value  
from analysis.tmp_rfm_recency r  
inner join analysis.tmp_rfm_frequency f on r.user_id =f.user_id 
inner join analysis.tmp_rfm_monetary_value m on r.user_id = m.user_id 