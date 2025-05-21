/*
 * Stored Procedure: silver.load_silver()

 * Description:
 * This stored procedure loads silver data from a several CSV files into the 
 * respective tables in the database.
 * It Truncates tables first. 
 * It utilizes the COPY command for efficient bulk loading of data.
 * The procedure measures and reports both the duration of the COPY operation in 
 * milliseconds and the total duration of the entire 
 * batch process in seconds. It also handles potential errors.
 *
 * Parameters:
 * - file_path: TEXT - The full path to the CSV file containing employee data.
 *
 * Usage:
 * CALL silver.load_silver()
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    affected_rows INTEGER;
    start_time TIMESTAMP;
    total_start_time TIMESTAMP;  
    end_time TIMESTAMP;
    total_end_time TIMESTAMP;
    duration INTERVAL;
    duration_ms DOUBLE PRECISION;
    total_duration INTERVAL;
    total_duration_seconds DOUBLE PRECISION;
BEGIN
RAISE NOTICE '===========================================';
RAISE NOTICE 'Loading Silver Layer';
RAISE NOTICE '===========================================';

RAISE NOTICE '-------------------------------------------';
RAISE NOTICE 'Loading CRM Tables';
RAISE NOTICE '-------------------------------------------';

     -- Capture the start time for the total process
     total_start_time := clock_timestamp();
     
     RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
     TRUNCATE TABLE silver.crm_cust_info;

     RAISE NOTICE '>> Inserting Data into Table: silver.crm_cust_info ';
     -- Capture the start time
     start_time := clock_timestamp();
    INSERT INTO silver.crm_cust_info (
   cst_id, 
   cst_key, 
   cst_firstname, 
   cst_lastname, 
   cst_marital_status, 
   cst_gndr,
   cst_create_date
  )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status, -- Normalize marital status values to readable format
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr, -- Normalize gender values to readable format
        cst_create_date
        FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1; -- Select the most recent record per customer
     -- Capture the end time
     end_time := clock_timestamp();

     -- Calculate the duration
     duration := end_time - start_time;

     -- Convert duration to milliseconds
     duration_ms := EXTRACT(EPOCH FROM duration) * 1000;

     GET DIAGNOSTICS affected_rows = ROW_COUNT;
     RAISE NOTICE '**** silver.crm_cust_info have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms; 
     
     RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
     TRUNCATE TABLE silver.crm_prd_info;

     RAISE NOTICE '>> Inserting Data into Table: silver.crm_prd_info ';
     -- Capture the start time
     start_time := clock_timestamp();
     INSERT INTO silver.crm_prd_info (
   prd_id,
   cat_id,
   prd_key,
   prd_nm,
   prd_cost,
   prd_line,
   prd_start_dt,
   prd_end_dt
  )
     SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
        SUBSTRING(prd_key, 7, length(prd_key)) AS prd_key,        -- Extract product key
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line, -- Map product line codes to descriptive values
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day'
            AS DATE
        ) AS prd_end_dt -- Calculate end date as one day before the next start date
        FROM bronze.crm_prd_info; 
     -- Capture the end time
     end_time := clock_timestamp();

     -- Calculate the duration
     duration := end_time - start_time;
   
     -- Convert duration to milliseconds
     duration_ms := EXTRACT(EPOCH FROM duration) * 1000;

     GET DIAGNOSTICS affected_rows = ROW_COUNT;
     RAISE NOTICE '**** silver.crm_prd_info have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms; 
     
     RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
     TRUNCATE TABLE silver.crm_sales_details;

     RAISE NOTICE '>> Inserting Data into Table: silver.crm_sales_details ';
     -- Capture the start time
     start_time := clock_timestamp();
     INSERT INTO silver.crm_sales_details (
   sls_ord_num,
   sls_prd_key,
   sls_cust_id,
   sls_order_dt,
   sls_ship_dt,
   sls_due_dt,
   sls_sales,
   sls_quantity,
   sls_price
     )
     SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt = 0 OR length(cast(sls_order_dt as VARCHAR)) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt = 0 OR length(cast(sls_order_dt as VARCHAR)) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt = 0 OR length(cast(sls_order_dt as VARCHAR)) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / COALESCE(sls_quantity, 0)
            ELSE sls_price  -- Derive price if original value is invalid
        END AS sls_price
        FROM bronze.crm_sales_details;
     -- Capture the end time
     end_time := clock_timestamp();

     -- Calculate the duration
     duration := end_time - start_time;

     -- Convert duration to milliseconds
     duration_ms := EXTRACT(EPOCH FROM duration) * 1000;

     GET DIAGNOSTICS affected_rows = ROW_COUNT;
     RAISE NOTICE '**** silver.crm_sales_details have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms;

RAISE NOTICE '-------------------------------------------';
RAISE NOTICE 'Loading ERP Tables';
RAISE NOTICE '-------------------------------------------';

     RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
     TRUNCATE TABLE silver.erp_cust_az12;

     RAISE NOTICE '>> Inserting Data into Table: silver.erp_cust_az12 ';
     -- Capture the start time
     start_time := clock_timestamp();
     INSERT INTO silver.erp_cust_az12 (
   cid,
   bdate,
   gen
  )
      SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, length(cid)) -- Remove 'NAS' prefix if present
            ELSE cid
        END AS cid, 
        CASE
            WHEN bdate > now() THEN NULL
            ELSE bdate
        END AS bdate, -- Set future birthdates to NULL
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen -- Normalize gender values and handle unknown cases
        FROM bronze.erp_cust_az12;
     -- Capture the end time
     end_time := clock_timestamp();

     -- Calculate the duration
     duration := end_time - start_time;

     -- Convert duration to milliseconds
     duration_ms := EXTRACT(EPOCH FROM duration) * 1000;

     GET DIAGNOSTICS affected_rows = ROW_COUNT;
     RAISE NOTICE '**** silver.erp_cust_az12 have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms;
     
     RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
     TRUNCATE TABLE silver.erp_loc_a101;

     RAISE NOTICE '>> Inserting Data into Table: silver.erp_loc_a101 ';
     -- Capture the start time
     start_time := clock_timestamp();
     INSERT INTO silver.erp_loc_a101 (
   cid,
   cntry
  )
     SELECT
        REPLACE(cid, '-', '') AS cid, 
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry -- Normalize and Handle missing or blank country codes
        FROM bronze.erp_loc_a101; 
     -- Capture the end time
     end_time := clock_timestamp();

     -- Calculate the duration
     duration := end_time - start_time;

     -- Convert duration to milliseconds
     duration_ms := EXTRACT(EPOCH FROM duration) * 1000;

     GET DIAGNOSTICS affected_rows = ROW_COUNT;
     RAISE NOTICE '**** silver.erp_loc_a101 have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms;
     
     RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
     TRUNCATE TABLE silver.erp_px_cat_g1v2;

     RAISE NOTICE '>> Inserting Data into Table: silver.erp_px_cat_g1v2 ';
     -- Capture the start time
     start_time := clock_timestamp();
     INSERT INTO silver.erp_px_cat_g1v2 (
   id,
   cat,
   subcat,
   maintenance
  )
     SELECT
        id,
        cat,
        subcat,
        maintenance
        FROM bronze.erp_px_cat_g1v2;
     -- Capture the end time
     end_time := clock_timestamp();

     -- Capture the end time
     total_end_time := clock_timestamp();

     -- Calculate the duration
     duration := end_time - start_time;

     
     -- Capture the end time for the total process
     total_duration := total_end_time - total_start_time;

     -- Convert duration to milliseconds
     duration_ms := EXTRACT(EPOCH FROM duration) * 1000;
 
     -- Convert total duration to seconds
     total_duration_seconds := EXTRACT(EPOCH FROM total_duration);

     GET DIAGNOSTICS affected_rows = ROW_COUNT;
     RAISE NOTICE '**** silver.erp_px_cat_g1v2 have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms;

     RAISE NOTICE '=====================================================';
     RAISE NOTICE '**** Total Load Duration: %.2f seconds', total_duration_seconds;
     RAISE NOTICE '=====================================================';

EXCEPTION
        WHEN others THEN
        RAISE NOTICE 'An unexpected error occurred: %', SQLERRM;

END;$procedure$
;
