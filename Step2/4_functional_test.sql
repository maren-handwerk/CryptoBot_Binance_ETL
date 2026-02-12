-- Comprehensive Functional Test: Linking all 3NF tables
-- This query proves the relational integrity of the database schema.

SELECT 
    c.Currency_Name AS 'Asset',
    c.Asset_Type AS 'Type',
    c.Category AS 'Market_Sector',
    p.Pair_Name AS 'Trading_Pair',
    ph.Price AS 'Market_Price',
    ph.Timestamp AS 'Retrieved_At'
FROM Price_Hist ph
JOIN Pair p ON ph.Pair_ID = p.Pair_ID
JOIN Currency c ON p.Currency_ID = c.Currency_ID
ORDER BY ph.Timestamp DESC;