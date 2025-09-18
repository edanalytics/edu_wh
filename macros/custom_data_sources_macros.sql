{%- macro add_cds_joins_v1(cds_model_config, driving_alias, join_cols) -%}
    {#- This macro ignores the new way of config -#}
    {#- Load custom data sources from var -#}
    {%- set custom_data_sources = var(cds_model_config, []) -%}

    {%- if custom_data_sources is not none and custom_data_sources | length -%}
        {%- for source_name, source_config in custom_data_sources.items() -%}
            {% if 'joins' not in source_config -%}
                left join {{ ref(source_name) }} as {{ source_name }}
                {%- for col in join_cols -%}
                    {% if loop.first %}
                        on {{ driving_alias }}.{{ col }} = {{ source_name }}.{{ col }}
                    {%- else %}
                        and {{ driving_alias }}.{{ col }} = {{ source_name }}.{{ col }}
                    {%- endif -%}
                {% endfor %}
            {%- endif %}
        {% endfor %}
    {%- endif %}
{%- endmacro -%}

{%- macro add_cds_joins_v2(cds_model_config) -%}
    {#- This macro only parses the new config -#}
    {#- Load custom data sources from var -#}
    {%- set custom_data_sources = var(cds_model_config, []) -%}

    {%- if custom_data_sources is not none and custom_data_sources | length -%}
        {%- for source_name, source_config in custom_data_sources.items() -%}
            {% if 'joins' in source_config -%}
                left join {{ ref(source_name) }} as {{ source_name }}
                {%- for join in source_config.joins -%}
                    {% if loop.first %}
                        on {{ join }}
                    {%- else %}
                        and {{ join }}
                    {%- endif -%}
                {% endfor %}
            {%- endif %}
        {% endfor %}
    {%- endif %}
{%- endmacro -%}

{%- macro add_cds_columns(cds_model_config) -%}
    {#- Load custom data sources from var -#}
    {%- set custom_data_sources = var(cds_model_config, []) -%}

    {% if custom_data_sources is not none and custom_data_sources | length %}
        {% for source_name, source_config in custom_data_sources.items() %}
            {# Check if 'joins' exist — this is the new config format #}
            {% if 'joins' in source_config %}
                {% for src_col_name, src_col_config in source_config.cds_additional_cols.items() %}
                    , coalesce({{ source_name }}.{{ src_col_name }}, {{ src_col_config.default }}) as {{ src_col_config.as }}
                {% endfor %}
            {% else %}
                {% for indicator_name, indicator_config in source_config.items() %}
                    , {{ source_name }}.{{ indicator_config.where }} as {{ indicator_name }}
                {% endfor %}
            {% endif %}
        {% endfor %}
    {% endif %}
{%- endmacro -%}