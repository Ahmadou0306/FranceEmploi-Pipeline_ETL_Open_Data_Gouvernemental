{{ config(materialized='table') }}

-- Série temporelle mensuelle par département avec évolution mois sur mois (M/M-1).
-- Permet de suivre la dynamique du marché du travail dans le temps.
-- Sources : int_territoire_chomage_population + int_territoire_offres_enrichies + dim_territoire

WITH base AS (
    SELECT
        c.annee_mois,
        c.code_departement,

        -- Indicateurs chômage
        c.nb_alloc,
        c.nb_od,
        c.nb_reprises,
        c.taux_chomage_indemnise,
        c.population_active,

        -- Agrégation des offres (plusieurs qualifications/types par département/mois)
        SUM(o.nb_offres_collectees) AS nb_offres_collectees,

        -- Tension du marché : offres disponibles pour 1000 actifs
        ROUND(
            SUM(o.nb_offres_collectees) / NULLIF(c.population_active, 0) * 1000,
            4
        ) AS offres_pour_1000_actifs

    FROM {{ ref('int_territoire_chomage_population') }} c
    -- Jointure temporelle stricte : même département, même mois
    LEFT JOIN {{ ref('int_territoire_offres_enrichies') }} o
        ON c.code_departement = o.code_departement
        AND c.annee_mois = o.date
    GROUP BY
        c.annee_mois,
        c.code_departement,
        c.nb_alloc,
        c.nb_od,
        c.nb_reprises,
        c.taux_chomage_indemnise,
        c.population_active
),

enriched AS (
    SELECT
        b.*,

        -- Dimensions géographiques via dim_territoire
        t.departement,
        t.code_region,
        t.region,

        -- Valeur du mois précédent pour calcul d'évolution
        LAG(b.nb_alloc) OVER (
            PARTITION BY b.code_departement ORDER BY b.annee_mois
        ) AS nb_alloc_mois_precedent

    FROM base b
    JOIN {{ ref('dim_territoire') }} t ON b.code_departement = t.code_departement
)

SELECT
    annee_mois,

    -- Dimensions
    code_departement,
    departement,
    code_region,
    region,

    -- Mesures chômage
    nb_alloc,
    nb_od,
    nb_reprises,
    taux_chomage_indemnise,
    population_active,

    -- Mesures offres
    nb_offres_collectees,
    offres_pour_1000_actifs,

    -- Évolution M/M-1 en valeur absolue et en pourcentage
    nb_alloc_mois_precedent,
    ROUND(
        (nb_alloc - nb_alloc_mois_precedent)
        / NULLIF(nb_alloc_mois_precedent, 0) * 100,
        2
    ) AS evolution_alloc_pct

FROM enriched
