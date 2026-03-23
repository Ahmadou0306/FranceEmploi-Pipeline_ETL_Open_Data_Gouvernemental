WITH source AS (
    SELECT *
    FROM {{source("raw_data","TRANCHE_AGE_RAW")}}
),

table_formatted AS (
    SELECT
        raw_data:"dimensions":"AGE"::STRING                   AS age,
        raw_data:"dimensions":"GEO"::STRING                   AS geo,
        raw_data:"dimensions":"RP_MEASURE"::STRING            AS rp_measure,
        raw_data:"dimensions":"SEX"::STRING                   AS sex,
        raw_data:"dimensions":"TIME_PERIOD"::STRING           AS time_period,
        raw_data:"measures":"OBS_VALUE_NIVEAU":"value"::FLOAT AS obs_value_niveau
    FROM source
)

SELECT *
FROM table_formatted