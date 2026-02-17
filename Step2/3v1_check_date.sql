-- This script verifies the data after the CMC Enrichment (Step 2)
USE CryptoBot_Step2;

-- 1. Check if the 8 Categories are appearing in the Pair table
-- This shows you which Pair belongs to which of your defined categories.
SELECT 
    Pair_Name, 
    Manual_Category 
FROM Pair
ORDER BY Manual_Category DESC;

-- 2. Audit Currency Table: Compare Asset Type and CMC Global Price
-- This helps you see if the global reference price was saved correctly.
SELECT 
    Currency_Name, 
    Asset_Type, 
    CMC_Global_Price_USD,
    CMC_Raw_Tags -- View the original tags provided by CMC
FROM Currency;

-- 3. The "Big Picture" Join: Pair, Price, and Category
-- This query shows you the Binance Price vs. the CMC Global Price for each pair.
SELECT 
    p.Pair_Name,
    p.Manual_Category AS Mapped_Category,
    ph.Price AS Binance_Price,
    c.CMC_Global_Price_USD AS CMC_Global_Avg,
    (ph.Price - c.CMC_Global_Price_USD) AS Price_Difference,
    ph.Timestamp
FROM Price_Hist ph
JOIN Pair p ON ph.Pair_ID = p.Pair_ID
JOIN Currency c ON p.Base_Currency_ID = c.Currency_ID
ORDER BY ph.Timestamp DESC
LIMIT 10;

-- 4. Summary: How many pairs are in each of your 8 categories?
-- Perfect for a quick status check of your project scope.
SELECT 
    Manual_Category, 
    COUNT(*) as Number_of_Pairs
FROM Pair
GROUP BY Manual_Category;