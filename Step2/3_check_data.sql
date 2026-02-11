SELECT c.Currency_Name, p.Pair_Name, ph.Price 
FROM Price_Hist ph 
JOIN Pair p ON ph.Pair_ID = p.Pair_ID 
JOIN Currency c ON p.Currency_ID = c.Currency_ID;