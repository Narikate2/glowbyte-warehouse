/*
===============================================================================
Скрипт DDL: Создание Gold Layer
===============================================================================
Назначение скрипта:
    скрипт создает представления для слоя Gold в хранилище данных. 
    Слой Gold представляет конечные таблицы измерений и фактов (звездообразная схема)

    Каждое представление выполняет преобразования и объединяет данные из слоя Silver
для получения чистого, расширенного и готового к использованию набора данных.

Использование:
    - Эти представления можно запрашивать напрямую для аналитики и составления отчетов.
===============================================================================
*/



-- =============================================================================
-- Создание Dimension Table:  gold.dim_products
-- =============================================================================

drop view if exists gold.dim_products
go

create view gold.dim_products as
select 
	ROW_NUMBER() over(order by prd_start_dt, prd_key) as product_key,
	prd_id as product_id,
	prd_key as product_number,
	prd_nm as product_name,
	cat_id as category_id,
	cat as category,
	subcat as subcategory,
	maintenance,
	prd_cost as cost,
	prd_line as product_line,
	prd_start_dt as start_date	
from silver.crm_prd_info pinf
left join silver.erp_px_cat_g1v2 px on pinf.cat_id = px.id
where prd_end_dt is null;
go


-- =============================================================================
-- Создание Fact Table: gold.fact_sales
-- =============================================================================

    
drop view if exists gold.fact_sales
go

create view gold.fact_sales as 
SELECT 
      sls_ord_num as order_number,
      pr.product_key,
      cu.customer_key,
      sls_order_dt as order_date,
      sls_ship_dt as shipping_date,
      sls_due_dt as due_date,
      sls_sales as sales_amount,
      sls_quantity as quantity,
      sls_price as price
 FROM silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id;
go



-- =============================================================================
-- Создание Dimension Table: gold.dim_customers
-- =============================================================================

    
drop view if exists gold.dim_customers
go
  
create view gold.dim_customers as 
select 
    row_number() over(order by cst_id) as customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_marital_status as last_name,
    la.cntry as country,
    case when ci.cst_gndr != 'n/a' then cst_gndr
        else isnull(ca.gen, 'n/a')
    end as gender,
    ci.cst_lastname as marital_status,
    ca.bdate as birthdate,
    ci.cst_create_date as create_date

from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
