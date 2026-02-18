-- This script creates the database and necessary tables
-- result: 3 tables in 3NF structure: Currency, Pair, Price_Hist

-- 1. create database
CREATE DATABASE IF NOT EXISTS CryptoBot_Step2;
USE CryptoBot_Step2;

-- optional: delete tables first if they already exist and you want to apply changes
--DROP TABLE IF EXISTS price_hist;
--DROP TABLE IF EXISTS pair;
--DROP TABLE IF EXISTS currency;
 
-- 2. create tables
CREATE TABLE IF NOT EXISTS Currency (
    Currency_ID INT AUTO_INCREMENT PRIMARY KEY,
    Currency_Name VARCHAR(50) NOT NULL UNIQUE,
    Asset_Type VARCHAR(20) NOT NULL,
    CMC_Global_Price_USD DECIMAL(20, 8), #Place for reference currency USD
    CMC_Raw_Tags TEXT
);

CREATE TABLE IF NOT EXISTS Pair (
    Pair_ID INT AUTO_INCREMENT PRIMARY KEY,
    Pair_Name VARCHAR(20) NOT NULL UNIQUE,
    Base_Currency_ID INT NOT NULL,
    Quote_Currency_ID INT NOT NULL,
    FOREIGN KEY (Base_Currency_ID) REFERENCES Currency(Currency_ID),
    FOREIGN KEY (Quote_Currency_ID) REFERENCES Currency(Currency_ID),
    Manual_Category VARCHAR(100) DEFAULT 'To be defined'
);

CREATE TABLE IF NOT EXISTS Price_Hist (
    Price_Hist_ID INT AUTO_INCREMENT PRIMARY KEY,
    Pair_ID INT NOT NULL,
    Price DECIMAL(20, 8) NOT NULL,
    Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (Pair_ID) REFERENCES Pair(Pair_ID)
);

-- 3. Show created schema
USE CryptoBot_Step2;

-- 3.1 show tables
SHOW tables;

-- 3.2 show columns of tables 
DESCRIBE Currency;
DESCRIBE Pair;
DESCRIBE Price_Hist;