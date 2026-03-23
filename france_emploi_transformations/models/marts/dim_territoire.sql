{{ config(materialized='table') }}

-- Référentiel géographique : un enregistrement par département
SELECT DISTINCT
    code_departement,
    departement,
    code_region,
    region
FROM {{ ref('offres_emploi_france_travail') }}
WHERE code_departement IS NOT NULL
