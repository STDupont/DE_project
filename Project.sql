
-- 2 Изучение данных нового источника

drop table if exists dwh.d_craftsman_2;
CREATE TABLE dwh.d_craftsman_2 (
    craftsman_id BIGINT PRIMARY KEY NOT NULL,
    craftsman_name TEXT NOT NULL,
    craftsman_address TEXT NOT null,
    craftsman_birthday Date not NULL,
    craftsman_email text not null,
    load_dttm Timestamp
);

insert into dwh.d_craftsman_2 (craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email)
select distinct craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email
from external_source.craft_products_orders;


drop table if exists dwh.d_product_2;
CREATE TABLE dwh.d_product_2(
	product_id BIGINT PRIMARY KEY NOT null,
	product_name text not NULL,
	product_description TEXT NOT null,
	product_type TEXT NOT null,
	product_price INT,
    load_dttm Timestamp
);    

insert into dwh.d_product_2 (product_id, product_name, product_description, product_type, product_price)
select distinct product_id, product_name, product_description, product_type, product_price
from external_source.craft_products_orders;



drop table if exists dwh.f_order_2;
CREATE TABLE dwh.f_order_2(
	order_id BIGINT PRIMARY KEY NOT null,
	order_customer_id BIGINT,
	order_product_id BIGINT,
	order_craftsman_id BIGINT,
	order_created_date Date,
	order_completion_date Date,
	order_status TEXT,
    load_dttm Timestamp
);

insert into dwh.f_order_2 (order_id, order_customer_id, order_product_id, order_craftsman_id, order_created_date, order_completion_date, order_status)
select distinct order_id, customer_id, product_id, craftsman_id, order_created_date, order_completion_date, order_status
from external_source.craft_products_orders;

		


drop table if exists dwh.d_customer_2;
create table dwh.d_customer_2 (
	customer_id BIGINT PRIMARY KEY NOT NULL,
    customer_name TEXT,
    customer_address TEXT NOT null,
    customer_birthday Date not NULL,
    customer_email text not null,
    load_dttm Timestamp
);

insert into dwh.d_customer_2 (customer_id, customer_name, customer_address, customer_birthday, customer_email)
select distinct customer_id, customer_name, customer_address, customer_birthday, customer_email
from external_source.customers;

---------------------------------------------------------
-- 3 Напишите скрипт переноса данных из источника в хранилище

MERGE INTO dwh.d_craftsman_2 d
USING (SELECT DISTINCT craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm FROM external_source.craft_products_orders) t
ON d.craftsman_name = t.craftsman_name AND d.craftsman_email = t.craftsman_email
WHEN MATCHED THEN
  UPDATE SET craftsman_id=t.craftsman_id, craftsman_name = t.craftsman_name, craftsman_address = t.craftsman_address, 
craftsman_birthday = t.craftsman_birthday, craftsman_email = t.craftsman_email, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
  VALUES (craftsman_id, t.craftsman_name, t.craftsman_address, t.craftsman_birthday, t.craftsman_email, current_timestamp);

MERGE INTO dwh.d_product_2 d
USING (SELECT DISTINCT product_id, product_name, product_description, product_type, product_price from external_source.craft_products_orders) t
ON d.product_name = t.product_name AND d.product_description = t.product_description AND d.product_price = t.product_price
WHEN MATCHED THEN
  UPDATE set product_id = t.product_id, product_type= t.product_type, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_id, product_name, product_description, product_type, product_price, load_dttm)
  VALUES (t.product_id, t.product_name, t.product_description, t.product_type, t.product_price, current_timestamp);

 
/* обновление существующих записей и добавление новых в dwh.d_customer */
MERGE INTO dwh.d_customer_2 d
USING (SELECT DISTINCT customer_id, customer_name, customer_address, customer_birthday, customer_email from external_source.customers) t
ON d.customer_name = t.customer_name AND d.customer_email = t.customer_email
WHEN MATCHED THEN
  UPDATE SET customer_id = t.customer_id, customer_address= t.customer_address, 
customer_birthday= t.customer_birthday, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (customer_id, customer_name, customer_address, customer_birthday, customer_email, load_dttm)
  VALUES (t.customer_id, t.customer_name, t.customer_address, t.customer_birthday, t.customer_email, current_timestamp);
 


/* обновление существующих записей и добавление новых в dwh.f_order */
MERGE INTO dwh.f_order_2 f
using (select distinct order_id, product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status from external_source.craft_products_orders) t
ON f.order_product_id = t.product_id AND f.order_craftsman_id = t.craftsman_id AND f.order_customer_id = t.customer_id AND f.order_created_date = t.order_created_date 
WHEN MATCHED THEN
  UPDATE SET order_id = t.order_id, order_completion_date = t.order_completion_date, order_status = t.order_status, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (order_id, order_product_id, order_craftsman_id, order_customer_id, order_created_date, order_completion_date, order_status, load_dttm)
  VALUES (t.order_id, t.product_id, t.craftsman_id, t.customer_id, t.order_created_date, t.order_completion_date, t.order_status, current_timestamp); 

-----------------------------------------------------------------------------------------------------
 -- 4 Изучите потребности бизнеса в новой витрине
 
 drop table if exists dwh.customer_report_datamart;
 create table dwh.customer_report_datamart (
 		id INT GENERATED ALWAYS AS identity not null,
 		customer_id INT,
 		customer_name TEXT,
 		customer_address TEXT,
 		customer_birthday date,
 		customer_email text,
 		customer_paid real,
 		customer_earned real,
 		customer_order_count INT,
 		customer_avg_price_per_month real,
 		median_completion_time_per_month INT,
 		customer_most_popular_product_per_month TEXT,
 		customer_most_popular_craftsman TEXT,
 		number_of_orders_created_per_month INT,
 		number_of_orders_in_progress_per_month INT,
 		number_of_orders_in_delivery_per_month INT,
 		number_of_orders_done_per_month INT,
 		number_of_orders_not_done_per_month INT,
 		report_month TEXT
 		);
 
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT null);
   
drop table if exists dwh.dwh_delta;

 create table if not exists dwh.dwh_delta as 
 SELECT	dcs2.customer_id,
 		dcs2.customer_name,
 		dcs2.customer_address,
 		dcs2.customer_birthday,
 		dcs2.customer_email,
 		fo2.order_id,
        dp2.product_id,
        dp2.product_name,
        dp2.product_price,
        dp2.product_type, 		
        fo2.order_completion_date,
 		fo2.order_created_date,
 		fo2.order_status AS order_status,
        TO_CHAR(fo2.order_created_date, 'yyyy-mm') AS report_month,
        crd.customer_id AS exist_customer_id,
        dc2.craftsman_name,
        dc2.load_dttm AS craftsman_load_dttm,
        dcs2.load_dttm AS customers_load_dttm,
        dp2.load_dttm as Products_load_dttm
 	from dwh.f_order_2 fo2
                INNER JOIN dwh.d_craftsman_2 dc2 ON fo2.order_craftsman_id = dc2.craftsman_id 
                INNER JOIN dwh.d_customer_2 dcs2 ON fo2.order_customer_id = dcs2.customer_id 
                INNER JOIN dwh.d_product_2 dp2 ON fo2.order_product_id = dp2.product_id 
				left join dwh.customer_report_datamart crd ON dcs2.customer_id = crd.customer_id
 			WHERE (fo2.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
	                (dc2.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
	                (dcs2.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
	                (dp2.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart	));
                           
drop table if exists dwh.dwh_update_delta;
                           
create table if not exists dwh.dwh_update_delta  as
select dd.customer_id
            FROM dwh.dwh_delta dd 
                WHERE dd.customer_id IS NOT null;  
                
                
	drop table if exists dwh.dwh_delta_insert_result;                
	
    create table if not exists dwh.dwh_delta_insert_result as     
    SELECT  
        t1.customer_id,
 		customer_name,
 		customer_address,
 		customer_birthday,
 		customer_email,
 		customer_paid,
 		customer_earned,
 		customer_order_count,
 		customer_avg_price_per_month,
 		median_completion_time_per_month,
 		customer_most_popular_product_per_month,
 		customer_most_popular_craftsman,
 		number_of_orders_created_per_month,
 		number_of_orders_in_progress_per_month,
 		number_of_orders_in_delivery_per_month,
 		number_of_orders_done_per_month,
 		number_of_orders_not_done_per_month, 		
 		t1.report_month
	 FROM (                
            SELECT -- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, кроме столбца с самой популярной категорией товаров у мастера. Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN
                t1.customer_id,
	 			t1.customer_name,
		 		t1.customer_address,
		 		t1.customer_birthday,
		 		t1.customer_email,
		 		SUM(t1.product_price) - (SUM(t1.product_price) * 0.1) as customer_paid,
		 		sum(t1.product_price)*0.1 as customer_earned,
		 		count(t1.order_id) as customer_order_count,
		 		avg(t1.product_price) as customer_avg_price_per_month,
		 		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY abs(cast(order_completion_date as date) - cast(order_created_date as date)))
		 			as median_completion_time_per_month, 
		 		sum(case when order_status = 'created' then 1 else 0 end) as number_of_orders_created_per_month,
		 		sum(case when order_status = 'in progress' then 1 else 0 end) as number_of_orders_in_progress_per_month,
		 		sum(case when order_status = 'delivery' then 1 else 0 end) as number_of_orders_in_delivery_per_month,
		 		sum(case when order_status = 'done' then 1 else 0 end) as number_of_orders_done_per_month,
		 		sum(case when order_status != 'done' then 1 else 0 end) as number_of_orders_not_done_per_month,
		 		t1.report_month
		 	from dwh.dwh_delta t1
		 	WHERE T1.exist_customer_id IS NULL
		 	group by T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_month) T1
		 left join 
		 		(select customer_id, product_name as customer_most_popular_product_per_month, report_month from
		 			( 
            		select customer_id, product_name, to_char(order_created_date, 'YYYY-MM') as report_month, count(*) as cr, 
				 			row_number() over (partition by customer_id, to_char(order_created_date, 'YYYY-MM') order by count(*) desc) as rn
				 	from dwh.dwh_delta
				 	group by customer_id, product_name, to_char(order_created_date, 'YYYY-MM')
					) s
				where rn = 1
		 		) t2
		 	on t2.customer_id = t1.customer_id and t1.report_month = t2.report_month
			left join 
		 		(select customer_id, report_month, craftsman_name as customer_most_popular_craftsman 
		 		from
		 			(
		 			select customer_id, craftsman_name, report_month, count(*) as cr, 
				 			row_number() over (partition by customer_id, report_month order by count(*) desc) as rn
				 	from dwh.dwh_delta
				 	group by customer_id, craftsman_name, report_month
					) s
				where rn = 1
		 		) t3
		 	on t3.customer_id = t1.customer_id and t3.report_month = t1.report_month;
		 			
	drop table if exists dwh.dwh_delta_update_result;
		 
	create table if not exists dwh.dwh_delta_update_result as
	select T2.customer_id,
            T2.customer_name,
            T2.customer_address,
            T2.customer_birthday,
            T2.customer_email,
            customer_paid,
	 		customer_earned,
	 		customer_order_count,
	 		customer_avg_price_per_month,
	 		median_completion_time_per_month,
	 		customer_most_popular_product_per_month,
	 		customer_most_popular_craftsman,
	 		number_of_orders_created_per_month,
	 		number_of_orders_in_progress_per_month,
	 		number_of_orders_in_delivery_per_month,
	 		number_of_orders_done_per_month,
	 		number_of_orders_not_done_per_month, 		
	 		t2.report_month
	 	from (
	 			select t1.customer_id,
			 			t1.customer_name,
				 		t1.customer_address,
				 		t1.customer_birthday,
				 		t1.customer_email,
				 		SUM(t1.product_price) - (SUM(t1.product_price) * 0.1) as customer_paid,
				 		sum(t1.product_price)*0.1 as customer_earned,
				 		count(t1.order_id) as customer_order_count,
				 		avg(t1.product_price) as customer_avg_price_per_month,
				 		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY abs(cast(order_completion_date as date) - cast(order_created_date as date)))
				 			as median_completion_time_per_month, 
				 		sum(case when order_status = 'created' then 1 else 0 end) as number_of_orders_created_per_month,
				 		sum(case when order_status = 'in progress' then 1 else 0 end) as number_of_orders_in_progress_per_month,
				 		sum(case when order_status = 'delivery' then 1 else 0 end) as number_of_orders_in_delivery_per_month,
				 		sum(case when order_status = 'done' then 1 else 0 end) as number_of_orders_done_per_month,
				 		sum(case when order_status != 'done' then 1 else 0 end) as number_of_orders_not_done_per_month,
				 		t1.report_month
				 	from (
				 			select cu.customer_id,
						            cu.customer_name,
						            cu.customer_address,
						            cu.customer_birthday,
						            cu.customer_email,
                                    fo.order_id AS order_id,
                                    pr.product_id AS product_id,
                                    pr.product_price AS product_price,
                                    pr.product_type AS product_type,
                                    fo.order_completion_date,
                                    fo.order_created_date,
                                    fo.order_status, 
                                    TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_month
                              from dwh.f_order_2 fo 
                                                INNER JOIN dwh.d_craftsman_2 cr ON fo.order_craftsman_id = cr.craftsman_id 
                                                INNER JOIN dwh.d_customer_2 cu ON fo.order_customer_id = cu.customer_id 
                                                INNER JOIN dwh.d_product_2 pr ON fo.order_product_id = pr.product_id
                                                INNER JOIN dwh.dwh_update_delta ud ON fo.order_customer_id = ud.customer_id
				 			) T1				 			
				 	group by customer_id, customer_name, customer_address, customer_birthday, customer_email,report_month
	 			) T2
			left join 
		 		(select customer_id, product_name as customer_most_popular_product_per_month, report_month from
		 			( 
            		select customer_id, product_name, to_char(order_created_date, 'YYYY-MM') as report_month, count(*) as cr, 
				 			row_number() over (partition by customer_id, to_char(order_created_date, 'YYYY-MM') order by count(*) desc) as rn
				 	from dwh.dwh_delta
				 	group by customer_id, product_name, to_char(order_created_date, 'YYYY-MM')
					) s
				where rn = 1
		 		) t3
		 	on t3.customer_id = t2.customer_id and t2.report_month = t3.report_month
			left join 
		 		(select customer_id, report_month, craftsman_name as customer_most_popular_craftsman 
		 		from
		 			(
		 			select customer_id, craftsman_name, report_month, count(*) as cr, 
				 			row_number() over (partition by customer_id, report_month order by count(*) desc) as rn
				 	from dwh.dwh_delta
				 	group by customer_id, craftsman_name, report_month
					) s
				where rn = 1
		 		) t4
		 	on t4.customer_id = t2.customer_id and t4.report_month = t2.report_month;  
		 		
		 		
			
   INSERT INTO dwh.customer_report_datamart (
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        customer_paid,
 		customer_earned,
 		customer_order_count,
 		customer_avg_price_per_month,
 		median_completion_time_per_month,
 		customer_most_popular_product_per_month,
 		customer_most_popular_craftsman,
 		number_of_orders_created_per_month,
 		number_of_orders_in_progress_per_month,
 		number_of_orders_in_delivery_per_month,
 		number_of_orders_done_per_month,
 		number_of_orders_not_done_per_month, 		
 		report_month
    ) SELECT 
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        customer_paid,
 		customer_earned,
 		customer_order_count,
 		customer_avg_price_per_month,
 		median_completion_time_per_month,
 		customer_most_popular_product_per_month,
 		customer_most_popular_craftsman,
 		number_of_orders_created_per_month,
 		number_of_orders_in_progress_per_month,
 		number_of_orders_in_delivery_per_month,
 		number_of_orders_done_per_month,
 		number_of_orders_not_done_per_month, 		
 		report_month
 	FROM dwh.dwh_delta_insert_result;

 		
 	UPDATE dwh.customer_report_datamart SET
        customer_name = updates.customer_name, 
        customer_address = updates.customer_address, 
        customer_birthday = updates.customer_birthday, 
        customer_email = updates.customer_email, 
        customer_paid = updates.customer_paid, 
        customer_earned = updates.customer_earned, 
        customer_order_count = updates.customer_order_count, 
        customer_avg_price_per_month = updates.customer_avg_price_per_month, 
        median_completion_time_per_month = updates.median_completion_time_per_month, 
        customer_most_popular_product_per_month = updates.customer_most_popular_product_per_month, 
        customer_most_popular_craftsman = updates.customer_most_popular_craftsman, 
        number_of_orders_created_per_month = updates.number_of_orders_created_per_month, 
        number_of_orders_in_progress_per_month = updates.number_of_orders_in_progress_per_month, 
        number_of_orders_in_delivery_per_month = updates.number_of_orders_in_delivery_per_month,
        number_of_orders_done_per_month = updates.number_of_orders_done_per_month, 
		number_of_orders_not_done_per_month = updates.number_of_orders_not_done_per_month,
        report_month = updates.report_month
    FROM (
        SELECT 
            customer_id,
	        customer_name,
	        customer_address,
	        customer_birthday,
	        customer_email,
	        customer_paid,
	 		customer_earned,
	 		customer_order_count,
	 		customer_avg_price_per_month,
	 		median_completion_time_per_month,
	 		customer_most_popular_product_per_month,
	 		customer_most_popular_craftsman,
	 		number_of_orders_created_per_month,
	 		number_of_orders_in_progress_per_month,
	 		number_of_orders_in_delivery_per_month,
	 		number_of_orders_done_per_month,
	 		number_of_orders_not_done_per_month, 		
	 		report_month
        FROM dwh.dwh_delta_update_result) AS updates
        inner join dwh.customer_report_datamart crd
		on crd.customer_id = updates.customer_id;	
 
	INSERT INTO dwh.load_dates_customer_report_datamart (
        load_dttm
    )
    SELECT GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
                    COALESCE(MAX(customers_load_dttm), NOW()), 
                    COALESCE(MAX(products_load_dttm), NOW())) 
        FROM dwh.dwh_delta;

 