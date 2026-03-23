-- Le score de déséquilibre ne peut pas être négatif
-- Retourne 0 ligne si OK
SELECT *
FROM {{ ref('mart_desequilibre_emploi') }}
WHERE score_desequilibre < 0
