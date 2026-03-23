-- Vérifie qu'il n'y a pas de gap > 1 mois dans la série temporelle par département
-- Retourne 0 ligne si OK
WITH serie AS (
    SELECT
        code_departement,
        annee_mois,
        LAG(annee_mois) OVER (PARTITION BY code_departement ORDER BY annee_mois) AS mois_precedent
    FROM {{ ref('mart_evolution_temporelle') }}
)

SELECT *
FROM serie
WHERE mois_precedent IS NOT NULL
  AND DATEDIFF('month', mois_precedent, annee_mois) > 1
