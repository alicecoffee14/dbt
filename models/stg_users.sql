/*-- This first query extracts the column names from a json file

{% set json_columns_query %}
SELECT
    raw:tableName::string as table_name, 
    key AS column_name, 
    object_keys(value)[0]::string AS type
FROM JOKO.RAW_SNOWPIPE_INGESTION.POC_INGESTION, 
LATERAL FLATTEN (input => raw:dynamodb:NewImage) 
WHERE table_name = 'users_test'
GROUP BY table_name, key, type
{% endset %}

{% set results = run_query(json_columns_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}*/


