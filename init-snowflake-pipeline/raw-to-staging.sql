-- ============================================================
-- TRANSFORMATION RAW → STAGING
--
-- Extrait les champs JSON depuis les tables _RAW (VARIANT)
-- vers des tables typées dans le schéma STAGING.
-- À exécuter après que Snowpipe ait alimenté les tables RAW.
-- ============================================================


-- Chômeurs indemnisés
-- Source : indemnisation chômage par département et mois
CREATE OR REPLACE TABLE FRANCE_EMPLOI_DB.STAGING.CHOMEURS_INDEMNISES AS
    SELECT
        raw_data:"aj_moy"::FLOAT                                    AS aj_moy,
        raw_data:"annee_mois"::STRING                               AS annee_mois,
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
    FROM FRANCE_EMPLOI_DB.STAGING.CHOMEURS_INDEMNISES_RAW;


-- Demandeurs d'emploi par tranche d'âge
-- Source : statistiques mensuelles par catégorie et tranche d'âge
CREATE OR REPLACE TABLE FRANCE_EMPLOI_DB.STAGING.DEMANDEURS_EMPLOI_TRANCHE_AGE AS
    SELECT
        raw_data:"age_detaille"::STRING                   AS age_detaille,
        raw_data:"categorie"::STRING                      AS categorie,
        raw_data:"champ"::STRING                          AS champ,
        raw_data:"date"::STRING                           AS date,
        raw_data:"nombre_de_demandeurs_d_emploi"::INTEGER AS nombre_de_demandeurs_d_emploi,
        raw_data:"type_de_donnees"::STRING                AS type_de_donnees
    FROM FRANCE_EMPLOI_DB.STAGING.DEMANDEURS_EMPLOI_TRANCHE_AGE_RAW;


-- Offres d'emploi France Travail
-- Source : volume d'offres par département, qualification et type d'emploi
CREATE OR REPLACE TABLE FRANCE_EMPLOI_DB.STAGING.OFFRES_EMPLOI_FRANCE_TRAVAIL AS
    SELECT
        raw_data:"code_departement"::STRING          AS code_departement,
        raw_data:"code_region"::STRING               AS code_region,
        raw_data:"date"::STRING                      AS date,
        raw_data:"departement"::STRING               AS departement,
        raw_data:"nombre_d_offres_d_emploi"::INTEGER AS nombre_d_offres_d_emploi,
        raw_data:"qualification"::STRING             AS qualification,
        raw_data:"region"::STRING                    AS region,
        raw_data:"type_d_emploi"::STRING             AS type_d_emploi,
        raw_data:"type_d_offre_d_emploi"::STRING     AS type_d_offre_d_emploi,
        raw_data:"type_de_donnees"::STRING           AS type_de_donnees
    FROM FRANCE_EMPLOI_DB.STAGING.OFFRES_EMPLOI_FRANCE_TRAVAIL_RAW;


-- Tranche d'âge (données INSEE)
-- Source : structure JSON imbriquée avec dimensions et mesures
CREATE OR REPLACE TABLE FRANCE_EMPLOI_DB.STAGING.TRANCHE_AGE AS
    SELECT
        raw_data:"dimensions":"AGE"::STRING                   AS age,
        raw_data:"dimensions":"GEO"::STRING                   AS geo,
        raw_data:"dimensions":"RP_MEASURE"::STRING            AS rp_measure,
        raw_data:"dimensions":"SEX"::STRING                   AS sex,
        raw_data:"dimensions":"TIME_PERIOD"::STRING           AS time_period,
        raw_data:"measures":"OBS_VALUE_NIVEAU":"value"::FLOAT AS obs_value_niveau
    FROM FRANCE_EMPLOI_DB.STAGING.TRANCHE_AGE_RAW;


-- ============================================================
-- CONTRÔLE — Vérification du contenu des tables STAGING
-- ============================================================

SELECT * FROM FRANCE_EMPLOI_DB.STAGING.CHOMEURS_INDEMNISES          LIMIT 10;
SELECT * FROM FRANCE_EMPLOI_DB.STAGING.DEMANDEURS_EMPLOI_TRANCHE_AGE LIMIT 10;
SELECT * FROM FRANCE_EMPLOI_DB.STAGING.OFFRES_EMPLOI_FRANCE_TRAVAIL  LIMIT 10;
SELECT * FROM FRANCE_EMPLOI_DB.STAGING.TRANCHE_AGE                   LIMIT 10;
