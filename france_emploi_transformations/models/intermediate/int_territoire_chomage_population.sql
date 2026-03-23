{{ config(materialized='view') }}

WITH chomeurs AS (
    SELECT * FROM {{ ref('chomeurs_indemnises') }}
),

-- Population active (15-64 ans) par département et année
pop_active AS (
    SELECT
        geo                          AS code_departement,
        CAST(time_period AS INTEGER) AS annee,
        SUM(obs_value_niveau)        AS population_active
    FROM {{ ref('tranche_age') }}
    WHERE age IN ('Y15T24', 'Y25T54', 'Y55T64')
      AND sex = 'T'
    GROUP BY geo, time_period
)

SELECT
    c.annee_mois,
    c.code_departement,
    c.departement,
    c.region,
    c.nb_alloc,
    c.nb_indemnises,
    c.nb_od,
    c.nb_reprises,
    c.aj_moy,
    c.depense,
    c.duree_moy,
    p.population_active,
    ROUND(c.nb_alloc / NULLIF(p.population_active, 0) * 100, 2) AS taux_chomage_indemnise
FROM chomeurs c
LEFT JOIN pop_active p
    ON  c.code_departement = p.code_departement
    AND YEAR(c.annee_mois) = p.annee
