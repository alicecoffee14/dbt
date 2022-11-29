-- within the stream we can have different subsequential changes for the same item: we only keep the most recent one
-- to do so we can apply a rank statement descending on creation time and with a partition on the primary key
WITH most_recent_changes AS (
    SELECT *,
            CONCAT(raw:dynamodb:Keys:userid:S::string, raw:dynamodb:Keys:timestamp:N::string) AS unique_primary_key,
            RANK() OVER ( PARTITION BY unique_primary_key
                            ORDER BY approximate_creation_time DESC ) AS ordering 
    FROM JOKO.RAW_SNOWPIPE_INGESTION.MyStream
    WHERE TRUE
            AND raw:tableName::string = 'two_primary_keys')
SELECT
    raw:dynamodb:Keys:userid:S::string userid,
    raw:dynamodb:Keys:timestamp:N::float timestamp, 
    raw:dynamodb:NewImage:test:S::string as test,
    raw:tableName::string AS table_name,
    action
FROM most_recent_changes
WHERE TRUE
    AND ordering = 1
