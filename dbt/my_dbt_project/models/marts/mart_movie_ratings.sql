{{ config(materialized='table') }}

SELECT
    MOVIE_ID,
    TITLE,
    COUNT(*) AS RATING_COUNT,
    AVG(RATING) AS AVG_RATING,
    MIN(RATING) AS MIN_RATING,
    MAX(RATING) AS MAX_RATING
FROM {{ ref('int_movie_ratings') }}
GROUP BY MOVIE_ID, TITLE
