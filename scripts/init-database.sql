/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'data-warehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'data-warehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Connect to the default database (usually 'postgres' in PostgreSQL)
\c postgres;

-- Drop and recreate the 'data-warehouse' database
DROP DATABASE IF EXISTS "data-warehouse";

-- Create the 'data-warehouse' database
CREATE DATABASE "data-warehouse";

-- Connect to the 'data-warehouse' database
\c "data-warehouse";

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
