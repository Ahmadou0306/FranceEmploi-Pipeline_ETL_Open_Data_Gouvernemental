-- ============================================================
-- FILE FORMAT
-- Définit comment Snowflake lit les fichiers JSON depuis GCS.
-- STRIP_OUTER_ARRAY = TRUE : si le JSON est un tableau [{...},{...}],
-- chaque élément est traité comme une ligne séparée.
-- ============================================================

CREATE OR REPLACE FILE FORMAT FRANCE_EMPLOI_DB.PUBLIC.JSON_FF
    TYPE         = 'JSON'
    NULL_IF      = ('')
    STRIP_OUTER_ARRAY = TRUE;


-- ============================================================
-- STAGES EXTERNES (pointeurs vers les dossiers GCS)
-- Chaque stage pointe vers le dossier /current/ du flux correspondant.
-- Pré-requis : gcs_snowpipe_integration doit être autorisé sur le bucket.
-- ============================================================

CREATE OR REPLACE STAGE FRANCE_EMPLOI_DB.PUBLIC.GCS_CHOMEURS_INDEMNISES_LOAD
    URL               = 'gcs://france-emploi-datawarehouse-data/raw/chomeurs_indemnises_departement/current/'
    STORAGE_INTEGRATION = gcs_snowpipe_integration
    FILE_FORMAT       = FRANCE_EMPLOI_DB.PUBLIC.JSON_FF;

CREATE OR REPLACE STAGE FRANCE_EMPLOI_DB.PUBLIC.GCS_DEMANDEURS_EMPLOI_TRANCHE_AGE_LOAD
    URL               = 'gcs://france-emploi-datawarehouse-data/raw/demandeurs_emploi_tranche_age/current/'
    STORAGE_INTEGRATION = gcs_snowpipe_integration
    FILE_FORMAT       = FRANCE_EMPLOI_DB.PUBLIC.JSON_FF;

CREATE OR REPLACE STAGE FRANCE_EMPLOI_DB.PUBLIC.GCS_OFFRES_EMPLOI_FRANCE_TRAVAIL_LOAD
    URL               = 'gcs://france-emploi-datawarehouse-data/raw/offres_emploi_france_travail/current/'
    STORAGE_INTEGRATION = gcs_snowpipe_integration
    FILE_FORMAT       = FRANCE_EMPLOI_DB.PUBLIC.JSON_FF;

CREATE OR REPLACE STAGE FRANCE_EMPLOI_DB.PUBLIC.GCS_TRANCHE_AGE_LOAD
    URL               = 'gcs://france-emploi-datawarehouse-data/raw/tranche_age/current/'
    STORAGE_INTEGRATION = gcs_snowpipe_integration
    FILE_FORMAT       = FRANCE_EMPLOI_DB.PUBLIC.JSON_FF;


-- ============================================================
-- VÉRIFICATION — Lister les fichiers disponibles dans chaque stage
-- Doit retourner les fichiers .json déposés par le DAG Airflow.
-- Si vide : le DAG n'a pas encore tourné ou le stage est mal configuré.
-- ============================================================

LIST @FRANCE_EMPLOI_DB.PUBLIC.GCS_CHOMEURS_INDEMNISES_LOAD;
LIST @FRANCE_EMPLOI_DB.PUBLIC.GCS_DEMANDEURS_EMPLOI_TRANCHE_AGE_LOAD;
LIST @FRANCE_EMPLOI_DB.PUBLIC.GCS_OFFRES_EMPLOI_FRANCE_TRAVAIL_LOAD;
LIST @FRANCE_EMPLOI_DB.PUBLIC.GCS_TRANCHE_AGE_LOAD;


-- ============================================================
-- PRÉVISUALISATION — Lecture directe depuis GCS (sans Snowpipe)
-- Utile pour valider le contenu avant d'ingérer dans les tables RAW.
-- ============================================================

-- Chômeurs indemnisés : JSON tableau, 1 objet = 1 ligne département/mois
SELECT
    $1:aj_moy::FLOAT                                    AS aj_moy,
    $1:annee_mois::STRING                               AS annee_mois,
    $1:code_departement::STRING                         AS code_departement,
    $1:departement::STRING                              AS departement,
    $1:depense::FLOAT                                   AS depense,
    $1:duree_moy::INTEGER                               AS duree_moy,
    $1:fdd::INTEGER                                     AS fdd,
    $1:montant_indemnisation_net_tous::FLOAT            AS montant_indemnisation_net_tous,
    $1:montant_indemnisation_net_travaille::FLOAT       AS montant_indemnisation_net_travaille,
    $1:montant_indemnisation_net_travaille_pas::FLOAT   AS montant_indemnisation_net_travaille_pas,
    $1:nb_alloc::INTEGER                                AS nb_alloc,
    $1:nb_indemnises::INTEGER                           AS nb_indemnises,
    $1:nb_indemnises_aref::INTEGER                      AS nb_indemnises_aref,
    $1:nb_indemnises_asp::INTEGER                       AS nb_indemnises_asp,
    $1:nb_od::INTEGER                                   AS nb_od,
    $1:nb_od_ini::INTEGER                               AS nb_od_ini,
    $1:nb_reprises::INTEGER                             AS nb_reprises,
    $1:part_travail::FLOAT                              AS part_travail,
    $1:part_travail_ind::FLOAT                          AS part_travail_ind,
    $1:region::STRING                                   AS region
FROM @FRANCE_EMPLOI_DB.PUBLIC.GCS_CHOMEURS_INDEMNISES_LOAD (PATTERN => '.*\\.json')
LIMIT 10;

-- Demandeurs d'emploi par tranche d'âge : JSON tableau
SELECT
    $1:age_detaille::STRING                   AS age_detaille,
    $1:categorie::STRING                      AS categorie,
    $1:champ::STRING                          AS champ,
    $1:date::STRING                           AS date,
    $1:nombre_de_demandeurs_d_emploi::INTEGER AS nombre_de_demandeurs_d_emploi,
    $1:type_de_donnees::STRING                AS type_de_donnees
FROM @FRANCE_EMPLOI_DB.PUBLIC.GCS_DEMANDEURS_EMPLOI_TRANCHE_AGE_LOAD (PATTERN => '.*\\.json')
LIMIT 10;

-- Offres d'emploi France Travail : JSON tableau
SELECT
    $1:code_departement::STRING          AS code_departement,
    $1:code_region::STRING               AS code_region,
    $1:date::STRING                      AS date,
    $1:departement::STRING               AS departement,
    $1:nombre_d_offres_d_emploi::INTEGER AS nombre_d_offres_d_emploi,
    $1:qualification::STRING             AS qualification,
    $1:region::STRING                    AS region,
    $1:type_d_emploi::STRING             AS type_d_emploi,
    $1:type_d_offre_d_emploi::STRING     AS type_d_offre_d_emploi,
    $1:type_de_donnees::STRING           AS type_de_donnees
FROM @FRANCE_EMPLOI_DB.PUBLIC.GCS_OFFRES_EMPLOI_FRANCE_TRAVAIL_LOAD (PATTERN => '.*\\.json')
LIMIT 10;

-- Tranche d'âge (INSEE) : JSON imbriqué (pas de tableau), 1 fichier = 1 objet
-- Structure : { dimensions: { AGE, GEO, ... }, measures: { OBS_VALUE_NIVEAU: { value } } }
SELECT
    $1:dimensions:AGE::STRING                     AS age,
    $1:dimensions:GEO::STRING                     AS geo,
    $1:dimensions:RP_MEASURE::STRING              AS rp_measure,
    $1:dimensions:SEX::STRING                     AS sex,
    $1:dimensions:TIME_PERIOD::STRING             AS time_period,
    $1:measures:OBS_VALUE_NIVEAU:value::FLOAT     AS obs_value_niveau
FROM @FRANCE_EMPLOI_DB.PUBLIC.GCS_TRANCHE_AGE_LOAD (PATTERN => '.*\\.json')
LIMIT 10;
