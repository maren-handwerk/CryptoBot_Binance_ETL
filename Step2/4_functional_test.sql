-- Funktionaler Test: Führt alle 3NF-Tabellen logisch zusammen
SELECT 
    p.Pair_Name, 
    ph.Price, 
    ph.Timestamp
FROM Price_Hist ph
JOIN Pair p ON ph.Pair_ID = p.Pair_ID