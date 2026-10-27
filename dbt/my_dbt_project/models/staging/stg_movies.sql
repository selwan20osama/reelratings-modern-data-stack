{{ config(materialized='view') }}

SELECT *
FROM REELRATINGS_DB.RAW.RAW_MOVIES
