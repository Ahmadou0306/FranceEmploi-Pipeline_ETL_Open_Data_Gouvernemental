{{ config(materialized='table') }}

-- Référentiel géographique : un enregistrement par département
SELECT DISTINCT
    code_departement,
    departement,
    code_region,
    region
FROM {{ ref('stg_offres_emploi_france_travail') }}
WHERE code_departement IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY code_departement ORDER BY code_region, departement) = 1
