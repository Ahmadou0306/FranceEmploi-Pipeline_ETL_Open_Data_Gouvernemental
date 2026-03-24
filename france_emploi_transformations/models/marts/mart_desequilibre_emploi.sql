{{ config(
    materialized='table',
    cluster_by=['annee_mois', 'code_departement']
) }}

-- Score de déséquilibre territorial mensuel par département.
-- Un score élevé = territoire sous tension (beaucoup de demandeurs, peu d'offres).
-- Formule : (nb_alloc / nb_offres) * (1 + taux_chomage / 100)
-- Sources : int_territoire_chomage_population + int_territoire_offres_enrichies + dim_territoire

WITH base AS (
    SELECT
        c.annee_mois,
        c.code_departement,

        -- Indicateurs chômage
        c.nb_alloc,
        c.taux_chomage_indemnise,
        c.population_active,

        -- Agrégation des offres sur le même mois (plusieurs lignes par département/mois possible)
        SUM(o.nb_offres_collectees) AS nb_offres

    FROM {{ ref('int_territoire_chomage_population') }} c
    -- Jointure temporelle stricte : même département, même mois
    LEFT JOIN {{ ref('int_territoire_offres_enrichies') }} o
        ON c.code_departement = o.code_departement
        AND c.annee_mois = o.date
    GROUP BY
        c.annee_mois,
        c.code_departement,
        c.nb_alloc,
        c.taux_chomage_indemnise,
        c.population_active
)

SELECT
    b.annee_mois,

    -- Dimensions géographiques via dim_territoire
    b.code_departement,
    t.departement,
    t.code_region,
    t.region,

    -- Mesures brutes
    b.nb_alloc,
    b.nb_offres,
    b.taux_chomage_indemnise,
    b.population_active,

    -- Ratio demandeurs / offres (NULL si nb_offres = 0)
    ROUND(b.nb_alloc / NULLIF(b.nb_offres, 0), 2) AS ratio_demandeurs_offres,

    -- Score composite pondéré par le taux de chômage
    ROUND(
        (b.nb_alloc / NULLIF(b.nb_offres, 0)) * (1 + b.taux_chomage_indemnise / 100),
        4
    ) AS score_desequilibre

FROM base b
JOIN {{ ref('dim_territoire') }} t ON b.code_departement = t.code_departement
