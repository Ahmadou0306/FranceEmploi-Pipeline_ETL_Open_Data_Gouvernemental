WITH source AS (
    SELECT *
    FROM {{source("raw_data","OFFRES_EMPLOI_FRANCE_TRAVAIL_RAW")}}
),

table_formatted AS (
    SELECT
        raw_data:"code_departement"::STRING          AS code_departement,
        raw_data:"code_region"::STRING               AS code_region,
        raw_data:"date"::DATE                        AS date,
        raw_data:"departement"::STRING               AS departement,
        raw_data:"nombre_d_offres_d_emploi"::INTEGER AS nombre_d_offres_d_emploi,
        raw_data:"qualification"::STRING             AS qualification,
        raw_data:"region"::STRING                    AS region,
        raw_data:"type_d_emploi"::STRING             AS type_d_emploi,
        raw_data:"type_d_offre_d_emploi"::STRING     AS type_d_offre_d_emploi,
        raw_data:"type_de_donnees"::STRING           AS type_de_donnees
    FROM source
)

SELECT *
FROM table_formatted