-- Règle métier : taux de chômage indemnisé entre 0% et 50%
-- Retourne 0 ligne si OK
SELECT *
FROM {{ ref('int_territoire_chomage_population') }}
WHERE taux_chomage_indemnise < 0
   OR taux_chomage_indemnise > 50
