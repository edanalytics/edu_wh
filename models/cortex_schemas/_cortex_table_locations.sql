{{
  config(
    materialized='ephemeral',
    tags=['cortex']
  )
}}

{#
  Ephemeral model that exposes each dbt model's resolved database/schema
  from the graph. Used by generate_cortex_schemas (Python model)
  to substitute base_table placeholders with actual locations.

  Includes all models across all packages. The Python model only looks up
  the table names that appear in its YAML seed, so extras are harmless.
#}

{% set rows = [] %}
{% for node in graph.nodes.values()
    if node.resource_type == 'model' %}
  {% do rows.append(node) %}
{% endfor %}

{% for node in rows %}
SELECT
  '{{ node.name | upper }}' AS table_name,
  '{{ node.database }}'     AS table_database,
  '{{ node.schema }}'       AS table_schema
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
