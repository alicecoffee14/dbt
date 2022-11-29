{{
    config(
        materialized='incremental',
        unique_key = ['first_key', 'second_key'],
        incremental_strategy='merge'
    )
}}

WITH

using_clause AS (

    SELECT
        raw:dynamodb:NewImage:userid:S::string as userid,
        raw:dynamodb:NewImage:age:N::string as age, 
        raw:dynamodb:NewImage:email:S::string as email, 
        raw:dynamodb:NewImage:test:S::string as test,
        raw:dynamodb:NewImage:json:M as json,
        raw:dynamodb:NewImage:bool:BOOL::boolean as bool

    FROM {{ ref('stream_logs') }}
    WHERE TRUE
        AND table_name = 'users_test'

   {% if is_incremental() %}

        WHERE approximate_creation_time > (SELECT max(approximate_creation_time) FROM {{ this }})

    {% endif %}
),

updates AS (

    SELECT
        *

    FROM using_clause

),

inserts AS (

    SELECT 
    *

    FROM using_clause

)

SELECT *
FROM updates

UNION 

SELECT *
FROM inserts