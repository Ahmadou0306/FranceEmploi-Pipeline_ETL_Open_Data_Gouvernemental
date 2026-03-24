{{ config(
    materialized='table',
    cluster_by=['code_departement']
) }}

-- Snapshot du marché du travail par département à la dernière période disponible.
-- Sert de vue synthétique pour les dashboards "état actuel" (une ligne par département).
-- Sources : int_territoire_chomage_population + int_territoire_offres_enrichies + dim_territoire

WITH latest_chomage AS (
    -- Dernière ligne disponible par département (chômage + population active)
    SELECT *
    FROM {{ ref('int_territoire_chomage_population') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY code_departement ORDER BY annee_mois DESC) = 1
),

latest_offres AS (
    -- Somme des offres collectées sur le dernier mois disponible par département
    SELECT
        code_departement,
        SUM(nb_offres_collectees) AS nb_offres_dernier_mois
    FROM {{ ref('int_territoire_offres_enrichies') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY code_departement ORDER BY date DESC) = 1
    GROUP BY code_departement
)

SELECT
    -- Clé de jointure vers dim_territoire
    c.code_departement,

    -- Dimensions géographiques (enrichies via dim_territoire)
    t.departement,
    t.code_region,
    t.region,

    -- Période de référence du snapshot
    c.annee_mois AS derniere_periode,

    -- Indicateurs démographiques
    c.population_active,

    -- Indicateurs chômage
    c.nb_alloc,
    c.nb_indemnises,
    c.taux_chomage_indemnise,
    c.aj_moy,
    c.depense,

    -- Indicateurs offres d'emploi
    o.nb_offres_dernier_mois

FROM latest_chomage c
-- Enrichissement géographique via la dimension territoire
JOIN {{ ref('dim_territoire') }} t ON c.code_departement = t.code_departement
-- LEFT JOIN car certains départements peuvent ne pas avoir d'offres sur la période
LEFT JOIN latest_offres o ON c.code_departement = o.code_departement
