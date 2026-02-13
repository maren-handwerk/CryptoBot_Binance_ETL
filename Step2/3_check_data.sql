USE CryptoBot_Step2;

SELECT 
    c_base.currency_name AS base_currency,
    c_quote.currency_name AS quote_currency,
    p.pair_name,
    ph.price,
    ph.timestamp
FROM Price_Hist ph
JOIN Pair p ON ph.pair_id = p.pair_id
JOIN Currency c_base ON p.base_currency_id = c_base.currency_id
JOIN Currency c_quote ON p.quote_currency_id = c_quote.currency_id;