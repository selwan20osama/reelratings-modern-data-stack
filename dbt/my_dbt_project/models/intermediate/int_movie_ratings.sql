{{ config(materialized='view') }}

SELECT
    m.MOVIEID AS MOVIE_ID,
    m.TITLE,
    r.RATING,
    r.USERID AS USER_ID
FROM {{ ref('stg_movies') }} AS m
INNER JOIN {{ ref('stg_ratings') }} AS r
    ON m.MOVIEID = r.MOVIEID
