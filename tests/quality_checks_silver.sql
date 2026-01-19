/*
===============================================================================
Проверка качества данных
===============================================================================
Назначение:
    Этот скрипт выполняет различные проверки качества на согласованность, точность
и стандартизацию данных на уровне "silver". Он включает в себя проверку на наличие:
    - Нулевых или повторяющихся первичных ключей.
    - Ненужных пробелов в строковых полях.
    - Стандартизацию и согласованность данных.
    - Недопустимые диапазоны дат и порядок следования дат.
    - Согласованность данных между связанными полями.

===============================================================================
*/

-- ====================================================================
-- Проверка 'silver.crm_cust_info'
-- ====================================================================
-- проверка на наличие дупликоватов ил NULL значений
-- ожидание: результатов нет
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- проверка на наличие пробелов
-- ожидание: результатов нет
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- проверка на стандартизированость данных и согласованность
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- ====================================================================
-- Проверка 'silver.crm_prd_info'
-- ====================================================================
-- проверка на наличие дупликоватов ил NULL значений
-- ожидание: результатов нет
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- проверка на наличие пробелов
-- ожидание: результатов нет
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- проверка на наличие NULL значений и отрицательных значечний в стоимости
-- ожидание: результатов нет
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- проверка на стандартизированость данных и согласованность
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Проверка на некорректность периода (Start Date > End Date)
-- ожидание: результатов нет
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Проверка 'silver.crm_sales_details'
-- ====================================================================
-- Проверка на некорректные значения
-- ожидание: результатов нет
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- проверка на неправильную дату заказа (Order Date > Shipping/Due Dates)
-- ожидание: результатов нет
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- проверка на согласованность данных: Sales = Quantity * Price
-- ожидание: результатов нет
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- проверка 'silver.erp_cust_az12'
-- ====================================================================
-- проверка дат, выходящих за пределы возможного
-- Ожидание: день рождения между 1924-01-01 и сегодня
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- проверка на стандартизированость данных и согласованность
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- проверка 'silver.erp_loc_a101'
-- ====================================================================
-- проверка на стандартизированость данных и согласованность
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- проверка 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- проверка на наличие пробелов
-- ожидание: результатов нет
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
