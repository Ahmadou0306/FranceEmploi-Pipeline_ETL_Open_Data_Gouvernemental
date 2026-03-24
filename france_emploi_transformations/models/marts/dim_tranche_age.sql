{{ config(materialized='table') }}

-- Référentiel des tranches d'âge avec libellés lisibles et indicateur population active
WITH tranches AS (
    SELECT DISTINCT age
    FROM {{ ref('stg_tranche_age') }}
)

SELECT
    age AS code_tranche_age,
    CASE age
        WHEN 'Y_LT15'  THEN 'Moins de 15 ans'
        WHEN 'Y15T24'  THEN '15 à 24 ans'
        WHEN 'Y25T54'  THEN '25 à 54 ans'
        WHEN 'Y55T64'  THEN '55 à 64 ans'
        WHEN 'Y_GE65'  THEN '65 ans et plus'
        ELSE age
    END AS libelle_tranche_age,
    CASE age
        WHEN 'Y15T24' THEN TRUE
        WHEN 'Y25T54' THEN TRUE
        WHEN 'Y55T64' THEN TRUE
        ELSE FALSE
    END AS est_population_active,
    CASE age
        WHEN 'Y_LT15' THEN 1
        WHEN 'Y15T24' THEN 2
        WHEN 'Y25T54' THEN 3
        WHEN 'Y55T64' THEN 4
        WHEN 'Y_GE65' THEN 5
        ELSE 99
    END AS ordre_affichage
FROM tranches
ORDER BY ordre_affichage
