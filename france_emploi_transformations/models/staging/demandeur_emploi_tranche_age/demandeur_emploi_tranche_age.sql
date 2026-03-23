WITH source AS (
    SELECT *
    FROM {{source("raw_data","DEMANDEURS_EMPLOI_TRANCHE_AGE_RAW")}}
),

table_formatted AS (
    SELECT
        raw_data:"age_detaille"::STRING                   AS age_detaille,
        raw_data:"categorie"::STRING                      AS categorie,
        raw_data:"champ"::STRING                          AS champ,
        raw_data:"date"::DATE                             AS date,
        raw_data:"nombre_de_demandeurs_d_emploi"::INTEGER AS nombre_de_demandeurs_d_emploi,
        raw_data:"type_de_donnees"::STRING                AS type_de_donnees
    FROM source
)

SELECT *
FROM table_formatted