{{
    config(
        materialized='incremental',
        unique_key='product_sk',
        incremental_strategy='merge',
        merge_update_columns=['valid_to', 'is_current']
    )
}}

WITH snapshot_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_valid_from']) }} AS product_sk,
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        dbt_valid_from AS valid_from,
        dbt_valid_to AS valid_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
        dbt_updated_at

    FROM {{ ref('snapshot_product') }}
)

SELECT * FROM snapshot_data

{% if is_incremental() %}
WHERE product_sk NOT IN (SELECT product_sk FROM {{ this }})
   OR dbt_updated_at > (SELECT COALESCE(MAX(dbt_updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
