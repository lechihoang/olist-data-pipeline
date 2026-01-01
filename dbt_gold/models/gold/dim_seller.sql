{{
    config(
        materialized='incremental',
        unique_key='seller_sk',
        incremental_strategy='merge',
        merge_update_columns=['valid_to', 'is_current']
    )
}}

WITH snapshot_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['seller_id', 'dbt_valid_from']) }} AS seller_sk,
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        geolocation_lat,
        geolocation_lng,
        dbt_valid_from AS valid_from,
        dbt_valid_to AS valid_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
        dbt_updated_at

    FROM {{ ref('snapshot_seller') }}
)

SELECT * FROM snapshot_data

{% if is_incremental() %}
WHERE seller_sk NOT IN (SELECT seller_sk FROM {{ this }})
   OR dbt_updated_at > (SELECT COALESCE(MAX(dbt_updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
