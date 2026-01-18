/*
===============================================================================
Хранимания процедура: загрузка серебряного слоя (бронзовый -> серебряный)
===============================================================================
Назначение:
    Хранимая процедура выполняет процесс ETL (извлечение, преобразование, загрузка) для
  заполнения таблиц схемы silver из схемы bronze.
	Выполняемые действия:
		- Очищает таблицы Silver.
		- Вставляет преобразованные и очищенные данные из Bronze таблиц в таблицы Silver.
		
Параметры:
	  Хранимая процедура не принимает никаких параметров и не возвращает никаких значений.

Пример использования:
    EXEC Silver.load_silver;
===============================================================================
*/

create or alter procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
	
	begin try
		set @batch_start_time = getdate()

		print '====================================';
		print 'Loading Silver layer';
		print '====================================';

		print '------------------------------------';
		print 'Loading data from CRM system'
		print '------------------------------------';

		set @start_time = getdate()
		print '>>Truncating silver.crm_prd_info';
		truncate table silver.crm_prd_info;

		print '>>Inserting into silver.crm_prd_info';
		insert into silver.crm_prd_info(
		  prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select 
	  		prd_id,
	  		replace(SUBSTRING(prd_key,1,5), '-', '_') category_id,
	  		SUBSTRING(prd_key, 7, len(prd_key)) prd_key,
	  		prd_nm,
	  		ISNULL(prd_cost, 0) prd_cost,
	  		case upper(trim(prd_line))
	  			when 'M' then 'Mountain'
	  			when 'R' then 'Road'
	  			when 'S' then 'Other Sales'
	  			when 'T' then 'Touring'
	  			else 'n/a'
	  		end as prd_line,
	  		prd_start_dt,
	  		dateadd(day, -1, lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt
		from bronze.crm_prd_info;
		set @end_time = getdate()

		print '>> load duration: ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '--------------------------'


		set @start_time = getdate()
		print '>>Truncating silver.crm_cust_info'
		truncate table silver.crm_cust_info

		print '>>Inserting into silver.crm_cust_info';
		insert into silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		select 
			cst_id,
			cst_key,
			trim(cst_firstname),
			trim(cst_lastname),
			case  
				when upper(trim(cst_marital_status)) = 'S' then 'Single'
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				else 'n/a'
			end as cst_marital_status,
			case  
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				when upper(trim(cst_gndr)) = 'F' then 'Female'
				else 'n/a'
			end as cst_gnrd,
			cst_create_date
		from
		(
		select
	  		*,
	  		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info
		where cst_id is not null
		) t where flag_last = 1;

		set @end_time = getdate()
		print '>> load duration: ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '--------------------------'


		set @start_time = getdate()
		print '>>Truncating silver.crm_sales_details'
		truncate table silver.crm_sales_details;

		print '>>Inserting into silver.crm_sales_details';
		insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		);

		select 
	  		sls_ord_num,
	  		sls_prd_key,
	  		sls_cust_id,
	  		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null 
	  			else cast(cast(sls_order_dt as varchar) as date)
	  		end sls_order_dt,
	  			case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null 
	  		else cast(cast(sls_ship_dt as varchar) as date)
	  		end sls_ship_dt,
	  		case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null 
	  			else cast(cast(sls_due_dt as varchar) as date)
	  		end sls_due_dt,
	  		case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
	  			then sls_quantity * abs(sls_price)
	  			else sls_sales
	  		end as sls_sales,
	  		sls_quantity,
	  		case when sls_price is null or sls_price <= 0
	  			then sls_sales / nullif(sls_quantity, 0)
	  			else sls_price
	  		end as sls_price
		from bronze.crm_sales_details;

		set @end_time = getdate()
		print '>> load duration: ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '--------------------------'


		print '------------------------------------';
		print 'Loading data from ERP system'
		print '------------------------------------';


		set @start_time = getdate()
		print '>>Truncating silver.erp_cust_az12'
		truncate table silver.erp_cust_az12;

		print '>>Inserting into silver.erp_cust_az12';
		insert into silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)

		select
	  		case when upper(cid) like 'NAS%' then substring(cid, 4, len(cid))
	  			else upper(cid)
	  		end as cid,
	  		case when bdate > getdate() then null
	  			else bdate
	  		end as bdate,
	  		case 
	  			when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
	  			when upper(trim(gen)) in ('M', 'Male') then 'Male'
	  			else 'n/a'
	  		end as gen
		from bronze.erp_cust_az12;

		set @end_time = getdate()
		print '>> load duration: ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '--------------------------'


		set @start_time = getdate()

		print '>>Truncating silver.erp_loc_a101'
		truncate table silver.erp_loc_a101;

		print '>>Inserting into silver.erp_loc_a101';
		insert into silver.erp_loc_a101
			(cid, cntry)

		select 
	  		replace(cid, '-', '') cid,
	  		case when trim(cntry) = 'DE' then 'Germany'
	  			 when trim(cntry) IN ('US', 'USA') then 'United States'
	  			 when trim(cntry) = '' or cntry is null then 'n/a'
	  			 else trim(cntry)
	  		end as cntry
		from bronze.erp_loc_a101;
		set @end_time = getdate()
		print '>> load duration: ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '--------------------------'


		set @start_time = getdate()

		print '>>Truncating silver.erp_px_cat_g1v2'
		truncate table silver.erp_px_cat_g1v2

		print '>>Inserting into silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2
			(id, cat, subcat, maintenance)
		select
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2;

		set @end_time = getdate()
		print '>> load duration: ' + cast(Datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '--------------------------'



		set @batch_end_time = getdate()

		print '========================================================'
		print 'Loading silver layer is completed. Time: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as varchar) + ' seconds'
		print '========================================================'

	end try

	begin catch
		print '====================================='
		print 'Error occured during loading silver layer'
		print 'error message' + Error_message()
		print 'error message' + cast(Error_number() as nvarchar);
		print 'error message' + cast(Error_state() as nvarchar);
		print '====================================='
	end catch;
end

exec silver.load_silver
