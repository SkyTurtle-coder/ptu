USE life_science_lecture_10;

-- Beispiel: Proteine je Organismus (Top 20)
SELECT o.scientific_name, COUNT(*) AS protein_count
FROM protein p
LEFT JOIN organism o ON p.organism_id = o.organism_id
GROUP BY o.scientific_name
ORDER BY protein_count DESC
LIMIT 20;

-- Beispiel: Proteine je Quelle
SELECT s.name AS source_name, COUNT(*) AS protein_count
FROM protein p
JOIN source_database s ON p.source_id = s.source_id
GROUP BY s.name
ORDER BY protein_count DESC;
