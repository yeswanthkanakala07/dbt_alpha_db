WITH dedup_query AS (
SELECT *,
        Row_Number() OVER (PARTITION BY id ORDER BY updateDate DESC) AS deduplication_id
FROM
     {{ source('source', 'items') }}
)

SELECT *
FROM dedup_query
WHERE deduplication_id = 1