-- DROP PROCEDURE bronze.load_bronze();

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
RAISE NOTICE 'Loading Bronze Layer';
RAISE NOTICE '===========================================';

RAISE NOTICE '-------------------------------------------';
RAISE NOTICE 'Loading CRM Tables';
RAISE NOTICE '-------------------------------------------';

     -- Capture the start time for the total process
     total_start_time := clock_timestamp();
     
     RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
     TRUNCATE TABLE bronze.crm_cust_info;

     RAISE NOTICE '>> Inserting Data into Table: bronze.crm_cust_info ';
     -- Capture the start time
     start_time := clock_timestamp();
     COPY bronze.crm_cust_info
     FROM 'D:\share\datasets\source_crm\cust_info.csv'
     DELIMITER ','
     CSV HEADER;
     -- Capture the end time
     end_time := clock_timestamp();

     -- Calculate the duration
     duration := end_time - start_time;

     -- Convert duration to milliseconds
     duration_ms := EXTRACT(EPOCH FROM duration) * 1000;

     GET DIAGNOSTICS affected_rows = ROW_COUNT;
     RAISE NOTICE '**** bronze.crm_cust_info have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms; 
     
     RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
     TRUNCATE TABLE bronze.crm_prd_info;

     RAISE NOTICE '>> Inserting Data into Table: bronze.crm_prd_info ';
     -- Capture the start time
     start_time := clock_timestamp();
     COPY bronze.crm_prd_info
     FROM 'D:\share\datasets\source_crm\prd_info.csv'
     DELIMITER ','
     CSV HEADER; 
     -- Capture the end time
     end_time := clock_timestamp();

     -- Calculate the duration
     duration := end_time - start_time;
   
     -- Convert duration to milliseconds
     duration_ms := EXTRACT(EPOCH FROM duration) * 1000;

     GET DIAGNOSTICS affected_rows = ROW_COUNT;
     RAISE NOTICE '**** bronze.crm_prd_info have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms; 
     
     RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
     TRUNCATE TABLE bronze.crm_sales_details;

     RAISE NOTICE '>> Inserting Data into Table: bronze.crm_sales_details ';
     -- Capture the start time
     start_time := clock_timestamp();
     COPY bronze.crm_sales_details
     FROM 'D:\share\datasets\source_crm\sales_details.csv'
     DELIMITER ','
     CSV HEADER; 
     -- Capture the end time
     end_time := clock_timestamp();

     -- Calculate the duration
     duration := end_time - start_time;

     -- Convert duration to milliseconds
     duration_ms := EXTRACT(EPOCH FROM duration) * 1000;

     GET DIAGNOSTICS affected_rows = ROW_COUNT;
     RAISE NOTICE '**** bronze.crm_sales_details have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms;

RAISE NOTICE '-------------------------------------------';
RAISE NOTICE 'Loading ERP Tables';
RAISE NOTICE '-------------------------------------------';

     RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
     TRUNCATE TABLE bronze.erp_cust_az12;

     RAISE NOTICE '>> Inserting Data into Table: bronze.erp_cust_az12 ';
     -- Capture the start time
     start_time := clock_timestamp();
     COPY bronze.erp_cust_az12
     FROM 'D:\share\datasets\source_erp\CUST_AZ12.csv'
     DELIMITER ','
     CSV HEADER; 
     -- Capture the end time
     end_time := clock_timestamp();

     -- Calculate the duration
     duration := end_time - start_time;

     -- Convert duration to milliseconds
     duration_ms := EXTRACT(EPOCH FROM duration) * 1000;

     GET DIAGNOSTICS affected_rows = ROW_COUNT;
     RAISE NOTICE '**** bronze.erp_cust_az12 have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms;
     
     RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
     TRUNCATE TABLE bronze.erp_loc_a101;

     RAISE NOTICE '>> Inserting Data into Table: bronze.erp_loc_a101 ';
     -- Capture the start time
     start_time := clock_timestamp();
     COPY bronze.erp_loc_a101
     FROM 'D:\share\datasets\source_erp\LOC_A101.csv'
     DELIMITER ','
     CSV HEADER; 
     -- Capture the end time
     end_time := clock_timestamp();

     -- Calculate the duration
     duration := end_time - start_time;

     -- Convert duration to milliseconds
     duration_ms := EXTRACT(EPOCH FROM duration) * 1000;

     GET DIAGNOSTICS affected_rows = ROW_COUNT;
     RAISE NOTICE '**** bronze.erp_loc_a101 have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms;
     
     RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
     TRUNCATE TABLE bronze.erp_px_cat_g1v2;

     RAISE NOTICE '>> Inserting Data into Table: bronze.erp_px_cat_g1v2 ';
     -- Capture the start time
     start_time := clock_timestamp();
     COPY bronze.erp_px_cat_g1v2
     FROM 'D:\share\datasets\source_erp\PX_CAT_G1V2.csv'
     DELIMITER ','
     CSV HEADER; 
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
     RAISE NOTICE '**** bronze.erp_px_cat_g1v2 have been loaded successfully. Affected rows: %, Duration: %.2f ms', affected_rows, duration_ms;

     RAISE NOTICE '=====================================================';
     RAISE NOTICE '**** Total Load Duration: %.2f seconds', total_duration_seconds;
     RAISE NOTICE '=====================================================';

EXCEPTION
        WHEN others THEN
        RAISE NOTICE 'An unexpected error occurred: %', SQLERRM;

END;$procedure$
;
