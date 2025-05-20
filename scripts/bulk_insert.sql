TRUNCATE TABLE crm_cust_info;
COPY crm_cust_info
FROM 'D:\share\datasets\source_crm\cust_info.csv'
DELIMITER ','
CSV HEADER; 

TRUNCATE TABLE crm_prd_info;
COPY crm_prd_info
FROM 'D:\share\datasets\source_crm\prd_info.csv'
DELIMITER ','
CSV HEADER; 

TRUNCATE TABLE crm_sales_details;
COPY crm_sales_details
FROM 'D:\share\datasets\source_crm\sales_details.csv'
DELIMITER ','
CSV HEADER; 

TRUNCATE TABLE erp_cust_az12;
COPY erp_cust_az12
FROM 'D:\share\datasets\source_erp\CUST_AZ12.csv'
DELIMITER ','
CSV HEADER; 

TRUNCATE TABLE erp_loc_a101;
COPY erp_loc_a101
FROM 'D:\share\datasets\source_erp\LOC_A101.csv'
DELIMITER ','
CSV HEADER; 

TRUNCATE TABLE erp_px_cat_g1v2;
COPY erp_px_cat_g1v2
FROM 'D:\share\datasets\source_erp\PX_CAT_G1V2.csv'
DELIMITER ','
CSV HEADER; 
