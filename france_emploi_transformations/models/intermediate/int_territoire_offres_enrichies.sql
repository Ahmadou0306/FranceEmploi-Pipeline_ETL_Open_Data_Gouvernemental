{{ config(materialized='view') }}

WITH offres AS (
    SELECT * FROM {{ ref('stg_offres_emploi_france_travail') }}
    WHERE type_d_offre_d_emploi = 'Offres collectées'
),

-- Population active (15-64 ans) par département et année
pop_active AS (
    SELECT
        geo                          AS code_departement,
        CAST(time_period AS INTEGER) AS annee,
        SUM(obs_value_niveau)        AS population_active
    FROM {{ ref('stg_tranche_age') }}
    WHERE age IN ('Y15T24', 'Y25T54', 'Y55T64')
      AND sex = 'T'
    GROUP BY geo, time_period
),

offres_aggregees AS (
    SELECT
        date,
        code_departement,
        departement,
        code_region,
        region,
        type_d_emploi,
        qualification,
        SUM(nombre_d_offres_d_emploi) AS nb_offres_collectees
    FROM offres
    GROUP BY date, code_departement, departement, code_region, region, type_d_emploi, qualification
)

SELECT
    o.*,
    p.population_active,
    ROUND(o.nb_offres_collectees / NULLIF(p.population_active, 0) * 1000, 4) AS offres_pour_1000_actifs
FROM offres_aggregees o
LEFT JOIN pop_active p
    ON  o.code_departement = p.code_departement
    AND YEAR(o.date)        = p.annee
