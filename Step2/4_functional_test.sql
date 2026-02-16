-- Comprehensive Functional Test: Linking all 3NF tables
-- This query proves the relational integrity of the database schema.

SELECT 
    c_base.currency_name AS 'Base_Asset',
    c_base.asset_type AS 'Base_Type',
    c_base.manual_category AS 'Base_Market_Sector',
    c_quote.currency_name AS 'Quote_Asset',
    c_quote.asset_type AS 'Quote_Type',
    c_quote.manual_category AS 'Quote_Market_Sector',
    p.pair_name AS 'Trading_Pair',
    ph.price AS 'Market_Price',
    ph.timestamp AS 'Retrieved_At'
FROM Price_Hist ph
JOIN Pair p ON ph.pair_id = p.pair_id
JOIN Currency c_base ON p.base_currency_id = c_base.currency_id
JOIN Currency c_quote ON p.quote_currency_id = c_quote.currency_id
ORDER BY ph.timestamp DESC;