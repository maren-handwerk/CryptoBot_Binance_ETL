/********************************************************************************
 * STEP 1: INFRASTRUCTURE SETUP
 * We first create the containers where our data will live.
 ********************************************************************************/

-- Create the database to isolate this project from other data
CREATE DATABASE IF NOT EXISTS CRYPTO_PROJECT;

-- Create a specific schema for our Star Schema (Clean, organized structure)
CREATE SCHEMA IF NOT EXISTS CRYPTO_PROJECT.STAR_SCHEMA;

-- Set the context so all following tables are created in the right place
USE SCHEMA CRYPTO_PROJECT.STAR_SCHEMA;


/********************************************************************************
 * STEP 2: DIMENSION TABLES
 * These tables store descriptive information about our data.
 ********************************************************************************/

-- DIM_TIME: Breaks down the timestamp for easy filtering (Yearly, Monthly, etc.)
CREATE TABLE IF NOT EXISTS DIM_TIME (
    TIME_ID TIMESTAMP_NTZ PRIMARY KEY, -- Unique ID (the exact second)
    DAY INT,
    MONTH INT,
    YEAR INT,
    QUARTER INT,
    HOUR INT,
    MINUTE INT,
    SECOND INT
);

-- DIM_CURRENCY: Stores asset details like 'BTC' or 'ETH'
CREATE TABLE IF NOT EXISTS DIM_CURRENCY (
    CURRENCY_ID INT PRIMARY KEY,
    CURRENCY_NAME VARCHAR(50),
    ASSET_TYPE VARCHAR(20)
);

-- DIM_CATEGORY: Market sectors (e.g., Stablecoin, Layer 1)
-- We use AUTOINCREMENT so Snowflake handles the IDs for us automatically.
CREATE TABLE IF NOT EXISTS DIM_CATEGORY (
    CATEGORY_ID INT AUTOINCREMENT PRIMARY KEY,
    CATEGORY_NAME VARCHAR(100)
);

-- DIM_SOURCE: Tracks where the data came from (e.g., Binance, CMC)
CREATE TABLE IF NOT EXISTS DIM_SOURCE (
    SOURCE_ID INT PRIMARY KEY,
    SOURCE_NAME VARCHAR(50)
);


/********************************************************************************
 * STEP 3: FACT TABLE
 * The central table containing our metrics (Price) and links to Dimensions.
 ********************************************************************************/

CREATE TABLE IF NOT EXISTS FACT_PAIRS (
    PAIR_ID INT,
    PAIR_NAME VARCHAR(20),
    CURRENCY_ID INT,        -- Links to DIM_CURRENCY
    TIME_ID TIMESTAMP_NTZ,  -- Links to DIM_TIME
    CATEGORY_ID INT,        -- Links to DIM_CATEGORY
    SOURCE_ID INT,          -- Links to DIM_SOURCE
    PRICE DECIMAL(20, 8),   -- Our main metric (The Binance Price)
    
    -- Defining Foreign Keys to ensure data integrity (optional but recommended)
    CONSTRAINT fk_time FOREIGN KEY (TIME_ID) REFERENCES DIM_TIME(TIME_ID),
    CONSTRAINT fk_currency FOREIGN KEY (CURRENCY_ID) REFERENCES DIM_CURRENCY(CURRENCY_ID),
    CONSTRAINT fk_category FOREIGN KEY (CATEGORY_ID) REFERENCES DIM_CATEGORY(CATEGORY_ID),
    CONSTRAINT fk_source FOREIGN KEY (SOURCE_ID) REFERENCES DIM_SOURCE(SOURCE_ID)
);

/********************************************************************************
 * 3.5 DATA Inserts
 * We need to put actual text into our dimensions before we can link them.
 ********************************************************************************/

-- 1. Create our Source entry
INSERT INTO DIM_SOURCE (SOURCE_ID, SOURCE_NAME)
SELECT 1, 'Binance' WHERE NOT EXISTS (SELECT 1 FROM DIM_SOURCE WHERE SOURCE_ID = 1);

-- 2. Create our Category entries
-- Category_ID is AUTOINCREMENT, so we only need to provide the Name
INSERT INTO DIM_CATEGORY (CATEGORY_NAME)
SELECT 'Layer 1' WHERE NOT EXISTS (SELECT 1 FROM DIM_CATEGORY WHERE CATEGORY_NAME = 'Layer 1');

INSERT INTO DIM_CATEGORY (CATEGORY_NAME)
SELECT 'Stablecoin' WHERE NOT EXISTS (SELECT 1 FROM DIM_CATEGORY WHERE CATEGORY_NAME = 'Stablecoin');

/********************************************************************************
 * 4. DATA LINKING (Updating the Fact Table)
 * Since CSV imports leave IDs empty, we link them now.
 ********************************************************************************/
-- Link all current records to 'Binance' (Source_ID 1)
-- remove duplicates by empty the tables
TRUNCATE TABLE DIM_CATEGORY;
TRUNCATE TABLE DIM_SOURCE;

-- fill in the dimensions
INSERT INTO DIM_CATEGORY (CATEGORY_NAME) VALUES ('Layer 1'), ('Stablecoin');
INSERT INTO DIM_SOURCE (SOURCE_ID, SOURCE_NAME) VALUES (1, 'Binance');


UPDATE FACT_PAIRS
SET CATEGORY_ID = (SELECT DISTINCT CATEGORY_ID FROM DIM_CATEGORY WHERE CATEGORY_NAME = 'Layer 1')
WHERE CURRENCY_ID IN (SELECT CURRENCY_ID FROM DIM_CURRENCY WHERE CURRENCY_NAME IN ('BTC', 'ETH', 'SOL'));

UPDATE FACT_PAIRS SET SOURCE_ID = 1 WHERE SOURCE_ID IS NULL;

-- Link BTC, ETH and SOL to 'Layer 1'
UPDATE FACT_PAIRS
SET CATEGORY_ID = (SELECT CATEGORY_ID FROM DIM_CATEGORY WHERE CATEGORY_NAME = 'Layer 1')
WHERE CURRENCY_ID IN (SELECT CURRENCY_ID FROM DIM_CURRENCY WHERE CURRENCY_NAME IN ('BTC', 'ETH', 'SOL'));

/********************************************************************************
 * 5. QUALITY CHECK
 * Final verification of our Star Schema
 ********************************************************************************/
SELECT 
    f.PRICE,
    c.CURRENCY_NAME,
    cat.CATEGORY_NAME,
    s.SOURCE_NAME,
    t.YEAR
FROM FACT_PAIRS f
JOIN DIM_CURRENCY c ON f.CURRENCY_ID = c.CURRENCY_ID
JOIN DIM_TIME t ON f.TIME_ID = t.TIME_ID
LEFT JOIN DIM_CATEGORY cat ON f.CATEGORY_ID = cat.CATEGORY_ID
LEFT JOIN DIM_SOURCE s ON f.SOURCE_ID = s.SOURCE_ID;