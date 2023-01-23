select count (client_id) total, count(distinct(client_id)) uniq 
from user_attributes ua;

SELECT coalesce (payment_amount,0) AS payment_amount 
FROM user_payment_log;

SELECT row_number() over(ORDER BY trip_no) num, 
 trip_no, id_comp
FROM trip
WHERE ID_comp < 3
ORDER BY id_comp, trip_no;

SELECT row_number() over(partition BY id_comp ORDER BY id_comp,trip_no) num,
 trip_no, id_comp 
FROM trip
WHERE ID_comp < 3
ORDER BY id_comp, trip_no;

select 
client_id, 
phone 
from 
(select *, row_number() over(partition by client_id ORDER BY created_at desc) num_c from user_contacts uc ) t 
where num_c = 1

select distinct on (client_id) client_id, phone from user_contacts uc order by client_id, created_at desc  

select translate(replace (phone,' ',''),'()','') from user_contacts uc

select substring(regexp_replace (phone,'[^\d]','','g'),2,3) from user_contacts uc

select updated_at, cast(DATE_TRUNC('month',TO_TIMESTAMP (updated_at,'HH24:MI:SS DD/MM/YYYY')) as date) 
from user_contacts uc2 

ALTER TABLE user_contacts ADD CONSTRAINT user_contacts_client_id_fkey foreign key (client_id) REFERENCES user_attributes (client_id)

ALTER TABLE user_contacts alter column client_id set not null

select cast(DATE_TRUNC('month',hitdatetime) as date) as month from user_activity_log ual 

select cast(DATE_TRUNC('month',TO_TIMESTAMP (hitdatetime,'HH24:MI:SS DD/MM/YYYY')) as date) from user_activity_log ual2 

select client_id, cast(DATE_TRUNC('month',hitdatetime) as date) as month,
       COUNT(CASE
                 WHEN action = 'visit' THEN 1
             END) visit_events,
       COUNT(CASE
                 WHEN action = 'registration' THEN 1
             END) registration_events,
       COUNT(CASE
                 WHEN action = 'login' THEN 1
             END) login_events 
   from user_activity_log ual 
   group by client_id, month 
   
   
SELECT client_id,
       avg(extract(epoch
                   FROM (hitdatetime - prev_hitdatetime))) AS events_delta_avg_sec
FROM
  (SELECT client_id,
          hitdatetime,
          lag(hitdatetime) OVER (PARTITION BY client_id
                                 ORDER BY hitdatetime) prev_hitdatetime
   FROM user_payment_log)t
GROUP BY client_id; 


   
select client_id,cast(DATE_TRUNC('month',hitdatetime) as date) as month, 
count (case when prev_hitdatetime = 'visit' and action = 'login' then 1 end) l_v 
from
(select client_id,
          hitdatetime, action,
          lag("action") OVER (PARTITION BY client_id
                                 ORDER BY hitdatetime) prev_hitdatetime
   FROM user_activity_log ual) t
   group by client_id, month
   
   
select client_id, count(1) from user_payment_log upl group by client_id  

select 
client_id, hitdatetime, DATE_TRUNC('hour',hitdatetime),
date_part('hour',DATE_TRUNC('hour',hitdatetime))
from user_payment_log upl where client_id =515537

select 
client_id, 
count(case when date_part('hour',DATE_TRUNC('hour',hitdatetime)) between 12 and 17 then 1 end)*100.0/count(1) as daily_actions_pct
from user_payment_log upl 
group by client_id 


SELECT 
client_id,
COUNT(case WHEN EXTRACT(hour FROM hitdatetime) BETWEEN 12 AND 17 THEN 1 END) * 100.0 / COUNT(1) AS daily_actions_pct
FROM user_payment_log
GROUP BY client_id

WITH cte AS (
    SELECT client_id,
        CAST(DATE_TRUNC('Month',hitdatetime) AS date) "month",
        COUNT(CASE WHEN "action" = 'visit' THEN 1 END) visit_events
    FROM user_activity_log
    WHERE EXTRACT(YEAR FROM hitdatetime) = 2021
    GROUP BY client_id, CAST(DATE_TRUNC('Month',hitdatetime) AS date)
)
SELECT 
min(visit_events) visit_events_min, 
max(visit_events) visit_events_max, 
avg(visit_events) visit_events_avg, 
count(case when visit_events is null then 1 end) visit_events_null, 
count(case when visit_events =0 then 1 end) visit_events_zero, 
count(case when visit_events =0 then 1 end)*100.0/count(1) visit_events_zero_pct,
count(1),
count(case when visit_events <>0 and visit_events is not null then 1 end)  visit_events_nonzero 
FROM cte;

WITH cte AS (
    SELECT client_id,
        CAST(DATE_TRUNC('Month',hitdatetime) AS date) "month",
        COUNT(CASE WHEN "action" = 'visit' THEN 1 END) visit_events
    FROM user_activity_log
    WHERE EXTRACT(YEAR FROM hitdatetime) = 2021
    GROUP BY client_id, CAST(DATE_TRUNC('Month',hitdatetime) AS date)
)
select * from cte 

drop table if exists clients_cluster_metrics_m

CREATE table if not exists clients_cluster_metrics_m (
month date, 
client_id bigint, 
utm_campaign varchar(30), 
reg_code varchar(3),
total_events bigint,
visit_events bigint,
registration_events bigint,
login_events bigint,
visit_to_login_events	bigint,
total_pay_events	bigint,
accepted_method_actions	bigint,
made_payments	bigint,
avg_payment double precision,
sum_payments double precision,
rejects_share double precision
)

ALTER TABLE clients_cluster_metrics_m  ADD CONSTRAINT clients_cluster_metrics_m_month_client_id_pkey primary key (month, client_id) 

ALTER TABLE clients_cluster_metrics_m DROP CONSTRAINT clients_cluster_metrics_m_client_id_fkey

ALTER TABLE clients_cluster_metrics_m  ADD CONSTRAINT clients_cluster_metrics_m_client_id_fkey  foreign key (client_id) references user_attributes (client_id) 


select indexdef from pg_catalog.pg_indexes pi2 where tablename = 'clients_cluster_metrics_m'

select * from pg_catalog.pg_indexes 

select * from clients_cluster_metrics_m

insert into clients_cluster_metrics_m (month, client_id, utm_campaign , reg_code , total_events , visit_events , registration_events , login_events , visit_to_login_events , 
total_pay_events, accepted_method_actions, avg_payment, made_payments,  sum_payments, rejects_share)
with act_lg as (
	select date_trunc('month',hitdatetime)::date "month",
		client_id,
		count(1) total_events,
		count(case when "action" = 'visit' then 1 end) visit_events,
		count(case when "action" = 'registration' then 1 end) registration_events,
		count(case when "action" = 'login' then 1 end) login_events,
		count(case when ("action" = 'login') and (prev_action = 'visit') then 1 end) visit_to_login_events
	from (
		select *,
			lag("action") over (partition by client_id order by hitdatetime) prev_action
		from user_activity_log
		where extract(year from hitdatetime) = 2021
			and "action" != 'N/A'
		)t
	group by 1,2
	),
pmnts as (
	select date_trunc('month',hitdatetime)::date "month",
		client_id,
		count(1) total_pay_events,
		count(case when "action" = 'accept-method' then 1 end) accepted_method_actions,
		count(case when "action" = 'make-payment' then 1 end) made_payments,
		avg(case when "action" = 'make-payment' then coalesce(payment_amount,0) else 0 end) avg_payment,
		sum(case when "action" = 'make-payment' then coalesce(payment_amount,0) else 0 end) sum_payments,
		sum(case when "action" = 'reject-payment' then coalesce(payment_amount,0) else 0 end)
			/ nullif(sum(case when "action" = 'make-payment' then coalesce(payment_amount,0) else 0 end),0) rejects_share
	from user_payment_log
	where extract(year from hitdatetime) = 2021
	group by 1,2
),
cntct as (
SELECT DISTINCT ON (client_id) client_id,
	SUBSTR(REGEXP_REPLACE(phone,'[^0123456789]','','g'),2,3) AS reg_code
FROM user_contacts
ORDER BY client_id,created_at DESC 
)
select coalesce(a."month",p."month") "month",
	ua.client_id,
	ua.utm_campaign,
	c.reg_code,
	coalesce(a.total_events,0) total_events,
	coalesce(a.visit_events,0) visit_events,
	coalesce(a.registration_events,0) registration_events,
	coalesce(a.login_events,0) login_events,
	coalesce(a.visit_to_login_events,0) visit_to_login_events,
	coalesce(p.total_pay_events,0) total_pay_events,
	coalesce(p.accepted_method_actions,0) accepted_method_actions,
	coalesce(p.avg_payment,0) avg_payment,
	coalesce(p.made_payments,0) made_payments,
	coalesce(p.sum_payments,0) sum_payments,
	p.rejects_share
from act_lg a
full join pmnts p on p."month" = a."month"
				and p.client_id = a.client_id
join user_attributes ua on ua.client_id = coalesce(a.client_id,p.client_id)
left join cntct c on c.client_id = ua.client_id
order by 1,2;

select month, count (1) total_records  from clients_cluster_metrics_m group by month order by month desc 
select 
month, 
count (case when rejects_share is null then 1 end)*100.0/count(1) rejects_share_empty_pct   
from clients_cluster_metrics_m group by month order by month desc 

--7.2.2
select DATE_TRUNC('Day',hitdatetime) "day", count(1) from user_activity_log ual group by day order by day

--7.3.1
select max("date") from load_dates ld 



with all_date as (select *, DATE_TRUNC('Day',hitdatetime) date_ from user_activity_log ual),
mx as (select max("date") mxd from load_dates ld)
select ad.id, ad.client_id, ad.hitdatetime, ad.action
from all_date ad 
left join load_dates on date_ = mx.mxd
group by ad.id, ad.client_id, ad.hitdatetime, ad.action
having DATE_TRUNC('Day',hitdatetime)> mx.mxd

--7.3.2
select ad.id, ad.client_id, ad.hitdatetime, ad.action
from user_activity_log ad 
group by ad.id, ad.client_id, ad.hitdatetime, ad.action
having DATE_TRUNC('Day',hitdatetime)> '2022-03-30' order by id

select *
from user_activity_log
where date_trunc('day', hitdatetime) >(select max(date) from load_dates)

--7.3.3
select *,
case when c.client_id is null then 'I' else 'U' end
from clients_inc ci left join clients c on ci.client_id =c.client_id 


insert into clients (client_id, client_firstname, client_lastname , client_email , client_phone , client_city , age)
select ci.client_id, ci.client_firstname, ci.client_lastname , ci.client_email , ci.client_phone , ci.client_city , ci.age
from clients_inc ci left join clients c on ci.client_id =c.client_id 
where c.client_id is null

select * from clients c 

flush_db.sh

--7.3.4
insert into clients
select client_id, client_firstname, client_lastname, client_email, client_phone, client_city, age 
from (
select ci.client_id, ci.client_firstname, ci.client_lastname, ci.client_email, ci.client_phone, ci.client_city, ci.age,
case 
	when c.age is null then 'I'
	else 'U'
end as "action"
from 
	clients_inc as ci
left outer join
	clients as c
on ci.client_id = c.client_id
and (ci.client_firstname <> c.client_firstname
or ci.client_lastname <> c.client_lastname
or ci.client_email <> c.client_email
or ci.client_phone <> c.client_phone
or ci.client_city <> c.client_city
or ci.age <> c.age)
) as inc
where "action" = 'I';

--7.3.5
with inc as (
select ci.*,
case
when c.client_id is null then 'i'
when ci.client_firstname <> c.client_firstname then 'u'
when ci.client_lastname <> c.client_lastname then 'u'
when ci.client_email <> c.client_email then 'u'
when ci.client_phone <> c.client_phone then 'u'
when ci.client_city <> c.client_city then 'u'
when ci.age <>c.age then 'u'
else 'no action' end as action
from clients_inc ci
left join clients c on c.client_id = ci.client_id)
update clients 
set client_firstname=inc.client_firstname,client_lastname=inc.client_lastname,client_email=inc.client_email,client_phone=inc.client_phone,client_city=inc.client_city,age=inc.age 
from inc where inc.action='u' and inc.client_id=clients.client_id



--7.4.1
INSERT INTO clients_cluster_metrics_m (
    month,
    client_id,
    utm_campaign,
    reg_code,
    total_events,
    visit_events,
    registration_events,
    login_events,
    visit_to_login_events,
    total_pay_events,
    accepted_method_actions,
    avg_payment,
    made_payments,
    sum_payments,
    rejects_share
)
WITH activity_stats AS (
    SELECT
        date_trunc('month', hitdatetime)::date                                          AS "month",
        client_id                                                                       AS client_id,
        COUNT(1)                                                                        AS total_events,
        SUM(CASE WHEN "action" = 'visit' THEN 1 ELSE 0 END)                             AS visit_events,
        SUM(CASE WHEN "action" = 'registration' THEN 1 ELSE 0 END)                      AS registration_events,
        SUM(CASE WHEN "action" = 'login' THEN 1 ELSE 0 END)                             AS login_events,
        SUM(CASE WHEN "action" = 'login' AND prev_action = 'visit' THEN 1 ELSE 0 END)   AS visit_to_login_events
    FROM (
        SELECT 
            *,
            LAG("action") OVER (PARTITION BY client_id ORDER BY hitdatetime ASC) AS prev_action
        FROM user_activity_log_arch
        WHERE 
            (extract(year FROM hitdatetime) = 2020 or extract(year FROM hitdatetime) = 2019)
            AND "action" != 'N/A'
        ) AS t
    GROUP BY 1,2
),
payment_stats AS (
    SELECT
        date_trunc('month',hitdatetime)::date AS "month",
        client_id AS client_id,
        count(1) AS total_pay_events,
        count(CASE WHEN "action" = 'accept-method' THEN 1 END) AS accepted_method_actions,
        count(CASE WHEN "action" = 'make-payment' THEN 1 END) AS made_payments,
        avg(CASE WHEN "action" = 'make-payment' THEN coalesce(payment_amount,0) ELSE 0 END) AS avg_payment,
        sum(CASE WHEN "action" = 'make-payment' THEN coalesce(payment_amount,0) ELSE 0 END) AS sum_payments,
        sum(CASE WHEN "action" = 'reject-payment' THEN coalesce(payment_amount, 0) ELSE 0 END)
            / nullif(sum(CASE WHEN "action" = 'make-payment' THEN coalesce(payment_amount,0) ELSE 0 END), 0)    AS rejects_share
    FROM user_payment_log_arch
    WHERE extract(year FROM hitdatetime) = 2020 or extract(year FROM hitdatetime) = 2019
    GROUP BY 1,2
),
user_contacts_latest AS (
    SELECT DISTINCT ON (client_id) 
        client_id,
	    SUBSTR(REGEXP_REPLACE(phone,'[^0123456789]','','g'),2,3) AS reg_code
    FROM user_contacts
    ORDER BY client_id ASC, created_at DESC 
)
SELECT 
    coalesce(a."month", p."month")          AS "month",
    ua.client_id                            AS client_id,
    ua.utm_campaign                         AS utm_campaign,
    contacts.reg_code                       AS reg_code,
    coalesce(a.total_events,0)              AS total_events,
    coalesce(a.visit_events,0)              AS visit_events,
    coalesce(a.registration_events, 0)      AS registration_events,
    coalesce(a.login_events, 0)             AS login_events,
    coalesce(a.visit_to_login_events, 0)    AS visit_to_login_events,
    coalesce(p.total_pay_events, 0)         AS total_pay_events,
    coalesce(p.accepted_method_actions, 0)  AS accepted_method_actions,
    coalesce(p.avg_payment, 0)              AS avg_payment,
    coalesce(p.made_payments, 0)            AS made_payments,
    coalesce(p.sum_payments, 0)             AS sum_payments,
    p.rejects_share                         AS rejects_share
FROM activity_stats AS a
    FULL JOIN payment_stats AS p 
        ON p."month" = a."month" AND p.client_id = a.client_id
    RIGHT JOIN user_attributes AS ua 
        ON ua.client_id = coalesce(a.client_id, p.client_id)
    LEFT JOIN user_contacts_latest AS contacts 
        ON contacts.client_id = ua.client_id
        where (p."month" is not null or a."month" is not null)
ORDER BY 1,2;

----------------------------------------------------------------------------------------------------------------------------------------------------------
--7.4.2
INSERT INTO clients_cluster_metrics_m (
    month,
    client_id,
    utm_campaign,
    reg_code,
    total_events,
    visit_events,
    registration_events,
    login_events,
    visit_to_login_events,
    total_pay_events,
    accepted_method_actions,
    avg_payment,
    made_payments,
    sum_payments,
    rejects_share
)
WITH activity_stats AS (
    SELECT
        date_trunc('month', hitdatetime)::date                                          AS "month",
        client_id                                                                       AS client_id,
        COUNT(1)                                                                        AS total_events,
        SUM(CASE WHEN "action" = 'visit' THEN 1 ELSE 0 END)                             AS visit_events,
        SUM(CASE WHEN "action" = 'registration' THEN 1 ELSE 0 END)                      AS registration_events,
        SUM(CASE WHEN "action" = 'login' THEN 1 ELSE 0 END)                             AS login_events,
        SUM(CASE WHEN "action" = 'login' AND prev_action = 'visit' THEN 1 ELSE 0 END)   AS visit_to_login_events
    FROM (
        SELECT 
            *,
            LAG("action") OVER (PARTITION BY client_id ORDER BY hitdatetime ASC) AS prev_action
        FROM user_activity_log
        WHERE date_trunc('day', hitdatetime) >(select max(date) from load_dates)
            AND "action" != 'N/A'
        ) AS t
    GROUP BY 1,2
),
payment_stats AS (
    SELECT
        date_trunc('month',hitdatetime)::date AS "month",
        client_id AS client_id,
        count(1) AS total_pay_events,
        count(CASE WHEN "action" = 'accept-method' THEN 1 END) AS accepted_method_actions,
        count(CASE WHEN "action" = 'make-payment' THEN 1 END) AS made_payments,
        avg(CASE WHEN "action" = 'make-payment' THEN coalesce(payment_amount,0) ELSE 0 END) AS avg_payment,
        sum(CASE WHEN "action" = 'make-payment' THEN coalesce(payment_amount,0) ELSE 0 END) AS sum_payments,
        sum(CASE WHEN "action" = 'reject-payment' THEN coalesce(payment_amount, 0) ELSE 0 END)
            / nullif(sum(CASE WHEN "action" = 'make-payment' THEN coalesce(payment_amount,0) ELSE 0 END), 0)    AS rejects_share
    FROM user_payment_log
         WHERE date_trunc('day', hitdatetime) >(select max(date) from load_dates)
    GROUP BY 1,2
),
user_contacts_latest AS (
    SELECT DISTINCT ON (client_id) 
        client_id,
	    SUBSTR(REGEXP_REPLACE(phone,'[^0123456789]','','g'),2,3) AS reg_code
    FROM user_contacts
    ORDER BY client_id ASC, created_at DESC 
)
SELECT 
    coalesce(a."month", p."month")          AS "month",
    ua.client_id                            AS client_id,
    ua.utm_campaign                         AS utm_campaign,
    contacts.reg_code                       AS reg_code,
    coalesce(a.total_events,0)              AS total_events,
    coalesce(a.visit_events,0)              AS visit_events,
    coalesce(a.registration_events, 0)      AS registration_events,
    coalesce(a.login_events, 0)             AS login_events,
    coalesce(a.visit_to_login_events, 0)    AS visit_to_login_events,
    coalesce(p.total_pay_events, 0)         AS total_pay_events,
    coalesce(p.accepted_method_actions, 0)  AS accepted_method_actions,
    coalesce(p.avg_payment, 0)              AS avg_payment,
    coalesce(p.made_payments, 0)            AS made_payments,
    coalesce(p.sum_payments, 0)             AS sum_payments,
    p.rejects_share                         AS rejects_share
FROM activity_stats AS a
    FULL JOIN payment_stats AS p 
        ON p."month" = a."month" AND p.client_id = a.client_id
    RIGHT JOIN user_attributes AS ua 
        ON ua.client_id = coalesce(a.client_id, p.client_id)
    LEFT JOIN user_contacts_latest AS contacts 
        ON contacts.client_id = ua.client_id
        where (p."month" is not null or a."month" is not null)
ORDER BY 1,2;

----------------------------------------------------------------------------------------------------------------------------------------------------------

with act_lg as (
    select date_trunc('month',hitdatetime)::date "month",
           client_id,
           count(1) total_events,
           count(case when "action" = 'visit' then 1 end) visit_events,
           count(case when "action" = 'registration' then 1 end) registration_events,
           count(case when "action" = 'login' then 1 end) login_events,
           count(case when ("action" = 'login') and (prev_action = 'visit') then 1 end) visit_to_login_events
    from (
             select *,
                    lag("action") over (partition by client_id order by hitdatetime) prev_action
             from public.user_activity_log
             where date_trunc('day', hitdatetime) >(select max(date) from public.load_dates)
               and "action" != 'N/A'
         )t
    group by 1,2
),
     pmnts as (
         select date_trunc('month',hitdatetime)::date "month",
                client_id,
                count(1) total_pay_events,
                count(case when "action" = 'accept-method' then 1 end) accepted_method_actions,
                count(case when "action" = 'make-payment' then 1 end) made_payments,
                avg(case when "action" = 'make-payment' then payment_amount else 0 end) avg_payment,
                sum(case when "action" = 'make-payment' then payment_amount else 0 end) sum_payments,
                sum(case when "action" = 'reject-payment' then payment_amount else 0 end)
                    / nullif(sum(case when "action" = 'make-payment' then payment_amount else 0 end),0) rejects_share
         from public.user_payment_log
         where date_trunc('day', hitdatetime) >(select max(date) from public.load_dates)
         group by 1,2
     ),
     cntct as (
         SELECT DISTINCT ON (client_id) client_id,
                                        SUBSTR(REGEXP_REPLACE(phone,'[^0123456789]','','g'),2,3) AS reg_code
         FROM user_contacts
         ORDER BY client_id,created_at DESC
     )
select coalesce(a."month",p."month") "month",
       ua.client_id,
       ua.utm_campaign,
       c.reg_code,
       coalesce(a.total_events,0) total_events,
       coalesce(a.visit_events,0) visit_events,
       coalesce(a.registration_events,0) registration_events,
       coalesce(a.login_events,0) login_events,
       coalesce(a.visit_to_login_events,0) visit_to_login_events,
       coalesce(p.total_pay_events,0) total_pay_events,
       coalesce(p.accepted_method_actions,0) accepted_method_actions,
       coalesce(p.avg_payment,0) avg_payment,
       coalesce(p.made_payments,0) made_payments,
       coalesce(p.sum_payments,0) sum_payments,
       p.rejects_share
from act_lg a
         full join pmnts p on p."month" = a."month"
    and p.client_id = a.client_id
         join user_attributes ua on ua.client_id = coalesce(a.client_id,p.client_id)
         left join cntct c on c.client_id = ua.client_id
order by 1,2;-- шаг 3 -это тоже в отдельное задание

select * from clients_cluster_metrics_m ccmm 

--7.4.3
insert into load_dates ("date", batch_id) select max(month), (select max(batch_id+1) from public.load_dates) from clients_cluster_metrics_m 




select * from lesson47.task3_record_log trl 

--8.1.1

ALTER TABLE lesson47.task3_record_log
ALTER COLUMN row_dttm SET DATA TYPE timestamp WITH time ZONE
USING to_timestamp(row_dttm, 'YYYY-MM-DD hh24:mi:ss') AT time ZONE 'Europe/Moscow'

 
SELECT row_dttm
FROM lesson47.task3_record_log AS t
WHERE row_id = 451; 

--8.1.2
alter table lesson47.task4_user_events add id int not null primary key GENERATED ALWAYS AS IDENTITY


select * from lesson47.task4_user_events tue 



docker run -d -p 15432:5432 -p 3000:3000 --name=de-project-sprint-1-server-local sindb/project-sprint-1:latest 









