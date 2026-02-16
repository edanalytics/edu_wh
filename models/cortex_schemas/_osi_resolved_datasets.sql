{{
  config(
    materialized='ephemeral',
    tags=['cortex']
  )
}}

{#
  Joins OSI dataset definitions with dbt graph metadata to resolve
  each dataset's physical database/schema/table in Snowflake.
  The Python model reads this to build Cortex base_table entries.
#}

SELECT
  d.semantic_model,
  d.name                AS dataset_name,
  d.source_model,
  d.primary_key,
  d.description,
  d.ai_context_synonyms,
  d.ai_context_instructions,
  COALESCE(l.table_database, CURRENT_DATABASE()) AS table_database,
  COALESCE(l.table_schema,   CURRENT_SCHEMA())   AS table_schema
FROM {{ ref('osi_datasets') }} d
LEFT JOIN {{ ref('_cortex_table_locations') }} l
  ON UPPER(d.source_model) = l.table_name
