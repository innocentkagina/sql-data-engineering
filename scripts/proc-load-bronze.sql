/* SQLINES DEMO *** ================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
-- SQLINES FOR EVALUATION USE ONLY (14 DAYS)
CREATE OR ALTER PROCEDURE bronze.load_bronze() AS $$
	DECLARE v_start_time TIMESTAMP(3); v_end_time TIMESTAMP(3); v_batch_start_time TIMESTAMP(3); v_batch_end_time TIMESTAMP(3);
BEGIN 
	BEGIN
		v_batch_start_time := NOW();
		RAISE NOTICE '%', '================================================';
		RAISE NOTICE '%', 'Loading Bronze Layer';
		RAISE NOTICE '%', '================================================';

		RAISE NOTICE '%', '------------------------------------------------';
		RAISE NOTICE '%', 'Loading CRM Tables';
		RAISE NOTICE '%', '------------------------------------------------';

		v_start_time := NOW();
		RAISE NOTICE '%', '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		RAISE NOTICE '%', '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT INTO bronze.crm_cust_info
		FROM 'C:sqldwh_projectdatasetssource_crmcust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		v_end_time := NOW();
		RAISE NOTICE '%', '>> Load Duration: ' || CAST(TRUNC(EXTRACT(EPOCH FROM DATE_TRUNC('second', v_end_time) - DATE_TRUNC('second', v_start_time))) AS VARCHAR(30)) || ' seconds';
		RAISE NOTICE '%', '>> -------------';

        v_start_time := NOW();
		RAISE NOTICE '%', '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		RAISE NOTICE '%', '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT INTO bronze.crm_prd_info
		FROM 'C:sqldwh_projectdatasetssource_crmprd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		v_end_time := NOW();
		RAISE NOTICE '%', '>> Load Duration: ' || CAST(TRUNC(EXTRACT(EPOCH FROM DATE_TRUNC('second', v_end_time) - DATE_TRUNC('second', v_start_time))) AS VARCHAR(30)) || ' seconds';
		RAISE NOTICE '%', '>> -------------';

        v_start_time := NOW();
		RAISE NOTICE '%', '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		RAISE NOTICE '%', '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT INTO bronze.crm_sales_details
		FROM 'C:sqldwh_projectdatasetssource_crmsales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		v_end_time := NOW();
		RAISE NOTICE '%', '>> Load Duration: ' || CAST(TRUNC(EXTRACT(EPOCH FROM DATE_TRUNC('second', v_end_time) - DATE_TRUNC('second', v_start_time))) AS VARCHAR(30)) || ' seconds';
		RAISE NOTICE '%', '>> -------------';

		RAISE NOTICE '%', '------------------------------------------------';
		RAISE NOTICE '%', 'Loading ERP Tables';
		RAISE NOTICE '%', '------------------------------------------------';
		
		v_start_time := NOW();
		RAISE NOTICE '%', '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		RAISE NOTICE '%', '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT INTO bronze.erp_loc_a101
		FROM 'C:sqldwh_projectdatasetssource_erploc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		v_end_time := NOW();
		RAISE NOTICE '%', '>> Load Duration: ' || CAST(TRUNC(EXTRACT(EPOCH FROM DATE_TRUNC('second', v_end_time) - DATE_TRUNC('second', v_start_time))) AS VARCHAR(30)) || ' seconds';
		RAISE NOTICE '%', '>> -------------';

		v_start_time := NOW();
		RAISE NOTICE '%', '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		RAISE NOTICE '%', '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT INTO bronze.erp_cust_az12
		FROM 'C:sqldwh_projectdatasetssource_erpcust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		v_end_time := NOW();
		RAISE NOTICE '%', '>> Load Duration: ' || CAST(TRUNC(EXTRACT(EPOCH FROM DATE_TRUNC('second', v_end_time) - DATE_TRUNC('second', v_start_time))) AS VARCHAR(30)) || ' seconds';
		RAISE NOTICE '%', '>> -------------';

		v_start_time := NOW();
		RAISE NOTICE '%', '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		RAISE NOTICE '%', '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT INTO bronze.erp_px_cat_g1v2
		FROM 'C:sqldwh_projectdatasetssource_erppx_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		v_end_time := NOW();
		RAISE NOTICE '%', '>> Load Duration: ' || CAST(TRUNC(EXTRACT(EPOCH FROM DATE_TRUNC('second', v_end_time) - DATE_TRUNC('second', v_start_time))) AS VARCHAR(30)) || ' seconds';
		RAISE NOTICE '%', '>> -------------';

		v_batch_end_time := NOW();
		RAISE NOTICE '%', '==========================================';
		RAISE NOTICE '%', 'Loading Bronze Layer is Completed';
        RAISE NOTICE '%', '   - Total Load Duration: ' || CAST(TRUNC(EXTRACT(EPOCH FROM DATE_TRUNC('SECOND', v_batch_end_time) - DATE_TRUNC('SECOND', v_batch_start_time))) AS VARCHAR(30)) || ' seconds';
		RAISE NOTICE '%', '==========================================';
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE '%', '==========================================';
		RAISE NOTICE '%', 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		RAISE NOTICE '%', 'Error Message' || SQLERRM;
		RAISE NOTICE '%', 'Error Message' || CAST (SQLSTATE AS VARCHAR(30));
		RAISE NOTICE '%', 'Error Message' || CAST (ERROR_STATE() AS VARCHAR(30));
		RAISE NOTICE '%', '==========================================';
	END;
END;
$$ LANGUAGE plpgsql;
