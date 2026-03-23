-- Règle métier : au moins 96 départements de France métropolitaine présents
-- Retourne 0 ligne si OK
SELECT 1 AS erreur
WHERE (
    SELECT COUNT(DISTINCT code_departement)
    FROM {{ ref('mart_territoire') }}
) < 96
