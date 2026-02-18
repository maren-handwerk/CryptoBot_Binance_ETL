-- Comprehensive Functional Test: Enriched with USD-Focus for Category Analysis
SELECT 
    c_base.currency_name AS 'Base_Asset',
    c_base.asset_type AS 'Base_Type',
    c_quote.currency_name AS 'Quote_Asset',
    c_quote.asset_type AS 'Quote_Type',
    p.pair_name AS 'Trading_Pair',
    p.manual_category AS 'Market_Sector',
    ph.price AS 'Binance_Price',
    c_base.CMC_Global_Price_USD AS 'CMC_Global_Ref',
    ph.timestamp AS 'Retrieved_At'
FROM Price_Hist ph
JOIN Pair p ON ph.pair_id = p.pair_id
JOIN Currency c_base ON p.base_currency_id = c_base.currency_id
JOIN Currency c_quote ON p.quote_currency_id = c_quote.currency_id
-- Filter in CMC_Global_Price_USD
WHERE c_quote.currency_name IN ('USDT', 'USD')
ORDER BY ph.timestamp DESC;