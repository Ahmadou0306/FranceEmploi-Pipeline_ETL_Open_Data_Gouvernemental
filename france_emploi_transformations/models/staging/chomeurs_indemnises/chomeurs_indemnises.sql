WITH source AS (
    SELECT *
    FROM {{source("raw_data","CHOMEURS_INDEMNISES_RAW")}}
),

table_formatted AS (
    SELECT
        raw_data:"aj_moy"::FLOAT                                    AS aj_moy,
        TO_DATE(raw_data:"annee_mois"::STRING, 'YYYY-MM')           AS annee_mois,
        raw_data:"code_departement"::STRING                         AS code_departement,
        raw_data:"departement"::STRING                              AS departement,
        raw_data:"depense"::FLOAT                                   AS depense,
        raw_data:"duree_moy"::INTEGER                               AS duree_moy,
        raw_data:"fdd"::INTEGER                                     AS fdd,
        raw_data:"montant_indemnisation_net_tous"::FLOAT            AS montant_indemnisation_net_tous,
        raw_data:"montant_indemnisation_net_travaille"::FLOAT       AS montant_indemnisation_net_travaille,
        raw_data:"montant_indemnisation_net_travaille_pas"::FLOAT   AS montant_indemnisation_net_travaille_pas,
        raw_data:"nb_alloc"::INTEGER                                AS nb_alloc,
        raw_data:"nb_indemnises"::INTEGER                           AS nb_indemnises,
        raw_data:"nb_indemnises_aref"::INTEGER                      AS nb_indemnises_aref,
        raw_data:"nb_indemnises_asp"::INTEGER                       AS nb_indemnises_asp,
        raw_data:"nb_od"::INTEGER                                   AS nb_od,
        raw_data:"nb_od_ini"::INTEGER                               AS nb_od_ini,
        raw_data:"nb_reprises"::INTEGER                             AS nb_reprises,
        raw_data:"part_travail"::FLOAT                              AS part_travail,
        raw_data:"part_travail_ind"::FLOAT                          AS part_travail_ind,
        raw_data:"region"::STRING                                   AS region
    FROM source
)

SELECT *
FROM table_formatted