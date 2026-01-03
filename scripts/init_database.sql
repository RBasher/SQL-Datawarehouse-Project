/* ============================================================
   PURPOSE:
   This script recreates the DataWareHouseProject database
   and sets up a Medallion Architecture:
   - Bronze: Raw data
   - Silver: Cleaned & transformed data
   - Gold: Business-ready data

   ⚠ WARNING:
   This script DELETES the database if it already exists.
   DO NOT run in production unless you are 100% sure.
   ============================================================ */

-- Switch to the master database
-- This is required because you cannot drop a database
-- while you are connected to it
USE master;
GO

-- Check if the database already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouseProject')
BEGIN
    /* 
       Set database to SINGLE_USER mode
       This forces all existing connections to close immediately
       ROLLBACK IMMEDIATE cancels open transactions
    */
    ALTER DATABASE DataWareHouseProject 
    SET SINGLE_USER 
    WITH ROLLBACK IMMEDIATE;

    -- Permanently delete the database
    DROP DATABASE DataWareHouseProject;
END;
GO

-- Create a fresh database
CREATE DATABASE DataWareHouseProject;
GO

-- Switch context to the new database
USE DataWareHouseProject;
GO

/* ============================================================
   SCHEMA CREATION (Medallion Architecture)
   ============================================================ */

-- BRONZE SCHEMA
-- Purpose: Store raw, unprocessed data exactly as received
CREATE SCHEMA bronze;
GO

-- SILVER SCHEMA
-- Purpose: Cleaned, standardized, and transformed data
CREATE SCHEMA silver;
GO

-- GOLD SCHEMA
-- Purpose: Aggregated, analytics-ready data for reporting
CREATE SCHEMA gold;
GO
