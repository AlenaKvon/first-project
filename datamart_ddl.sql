create table analysis.dm_rfm_segments ( 
user_id int not null primary key, 
recency int  NOT NULL CHECK(recency >= 1 AND recency <= 5),
frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5),
monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5));