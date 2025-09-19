{%- macro cds_depends_on(cds_model_config) -%}
    {%- set cds = var(cds_model_config, {}) -%}
        {%- if cds is mapping and cds|length -%}
            {%- for source_name, _ in cds.items() -%}
-- depends_on: {{ ref(source_name) }}
            {%- endfor -%}
    {%- endif -%}
{%- endmacro -%}

{%- macro add_cds_joins_v1(custom_data_sources, driving_alias, join_cols) -%}
    {#-
      Expected "old" config shape per source:
        <source_name>: {
          <indicator_name>: { where: <col_or_expr> },
          ...
        }

      Only sources WITHOUT 'joins' key are considered "old" and handled here.
    -#}

    {% if custom_data_sources is mapping and custom_data_sources|length %}
        {% for source_name, source_config in custom_data_sources|dictsort %}
            {% if 'joins' not in source_config %}
                left join {{ ref(source_name) }} as {{ source_name }}
                {% for col in join_cols %}
                    {% if loop.first %}
                        on {{ driving_alias }}.{{ col }} = {{ source_name }}.{{ col }}
                    {% else %}
                        and {{ driving_alias }}.{{ col }} = {{ source_name }}.{{ col }}
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endfor %}
    {% endif %}
{%- endmacro -%}

{%- macro add_cds_joins_v2(custom_data_sources) -%}
    {#-
      Expected "new" config shape per source:
        <source_name>: {
          joins: [ "<expr1>", "<expr2>", ... ],
          cds_additional_cols: {
            <src_col_name>: { as: <alias>, default: <literal_or_expr> },
            ...
          }
        }

      Only sources WITH 'joins' key are handled here.
    -#}

    {% if custom_data_sources is mapping and custom_data_sources|length %}
        {% for source_name, source_config in custom_data_sources|dictsort %}
            {% if 'joins' in source_config and source_config.joins %}
                left join {{ ref(source_name) }} as {{ source_name }}
                {% for join in source_config.joins %}
                    {% if loop.first %}
                        on {{ join }}
                    {% else %}
                        and {{ join }}
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endfor %}
    {% endif %}
{%- endmacro -%}

{%- macro add_cds_columns(custom_data_sources) -%}
    {#-
      Old format per source:
        <source_name>: {
          <indicator_name>: { where: <col_or_expr> },
          ...
        }
      Emits:
        , <source_name>.<where> as <indicator_name>

      New format per source:
        <source_name>: {
          joins: [...],
          cds_additional_cols: {
            <src_col_name>: { as: <alias>, default: <literal_or_expr> },
            ...
          }
        }
      Emits:
        , coalesce(<source_name>.<src_col_name>, <default>) as <alias>
    -#}

    {% if custom_data_sources is mapping and custom_data_sources|length %}
        {% for source_name, source_config in custom_data_sources|dictsort %}
            {% if 'joins' in source_config %}
                {# New config pathway #}
                {% if 'cds_additional_cols' in source_config and source_config.cds_additional_cols %}
                    {% for src_col_name, src_col_config in source_config.cds_additional_cols.items() %}
                        , coalesce({{ source_name }}.{{ src_col_name }}, {{ src_col_config.default }}) as {{ src_col_config.as }}
                    {% endfor %}
                {% endif %}
            {% else %}
                {# Old config pathway #}
                {% for indicator_name, indicator_config in source_config.items() %}
                    , {{ source_name }}.{{ indicator_config.where }} as {{ indicator_name }}
                {% endfor %}
            {% endif %}
        {% endfor %}
    {% endif %}
{%- endmacro -%}
