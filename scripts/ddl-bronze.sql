/* SQLINES DEMO *** ================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

    DROP TABLE IF EXISTS bronze.crm-cust-info;
 

-- SQLINES FOR EVALUATION USE ONLY (14 DAYS)
CREATE TABLE bronze.crm-cust-info (
    cst-id              INT,
    cst-key             VARCHAR(50),
    cst-firstname       VARCHAR(50),
    cst-lastname        VARCHAR(50),
    cst-marital-status  VARCHAR(50),
    cst-gndr            VARCHAR(50),
    cst-create-date     DATE
);

    DROP TABLE IF EXISTS bronze.crm-prd-info;
 

CREATE TABLE bronze.crm-prd-info (
    prd-id       INT,
    prd-key      VARCHAR(50),
    prd-nm       VARCHAR(50),
    prd-cost     INT,
    prd-line     VARCHAR(50),
    prd-start-dt TIMESTAMP(3),
    prd-end-dt   TIMESTAMP(3)
);

    DROP TABLE IF EXISTS bronze.crm-sales-details;
 

CREATE TABLE bronze.crm-sales-details (
    sls-ord-num  VARCHAR(50),
    sls-prd-key  VARCHAR(50),
    sls-cust-id  INT,
    sls-order-dt INT,
    sls-ship-dt  INT,
    sls-due-dt   INT,
    sls-sales    INT,
    sls-quantity INT,
    sls-price    INT
);

    DROP TABLE IF EXISTS bronze.erp-loc-a101;
 

CREATE TABLE bronze.erp-loc-a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);

    DROP TABLE IF EXISTS bronze.erp-cust-az12;
 

CREATE TABLE bronze.erp-cust-az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);

    DROP TABLE IF EXISTS bronze.erp-px-cat-g1v2;
 

CREATE TABLE bronze.erp-px-cat-g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);
