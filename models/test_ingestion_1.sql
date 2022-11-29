-- build this as a table
{{ config(materialized='table') }}

SELECT *
FROM JOKO.RAW_SNOWPIPE_INGESTION.USERS_TRANSFORMED