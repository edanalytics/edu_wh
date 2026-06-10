{#-
    Custom data sources let stadium repos append columns from their own dbt models to core
    warehouse tables without modifying this package.

    How it works:
        1. Add a source config in dbt_project.yml under the models `edu:stu_demos:custom_data_sources` var key.
        2. The warehouse model calls `add_custom_data_source(...)` just before its final SELECT.
        3. The macro wraps the base CTE in a `add_custom_data_source` CTE that left-joins each configured
        source and appends its columns. If nothing is configured, it passes through without changes.

    Two YAML config formats are supported:

        Original-format YAML config (uses a `where` expression per column):
        ```YAML
            vars:
                edu:stu_demos:custom_data_sources:
                    bld_ef3__ell_annual:               # name of a dbt model in the stadium repo
                    is_english_language_learner_annual:
                        where: "coalesce(is_ell_annual, False)"
        ```

        New-format YAML config (uses explicit join expressions and column aliases):
        ```YAML
            vars:
                edu:stu_demos:custom_data_sources:
                    bld_ef3__ell_annual:
                        joins:
                            - "formatted.k_student = bld_ef3__ell_annual.k_student"
                        add_cols:
                            is_ell_annual: { as: is_english_language_learner_annual, default: false }
        ```

    Model usage (same for both formats):
        ...
        formatted as (...SQL...)
        {{ add_custom_data_source('edu:stu_demos:custom_data_sources', join_cols=['k_student']) }}
        select * from add_custom_data_source

    Macros in this file:
        add_custom_data_source         : wraps base CTE in add_custom_data_source.
        add_custom_data_source_joins   : appends LEFT JOIN clauses.
        add_custom_data_source_columns : appends column expressions.
        custom_data_source_depends_on  : appends depends_on comments for dbts static parser.
-#}


{%- macro custom_data_source_depends_on(custom_data_source_model_config) -%}
    {#-
      Appends a `-- depends_on: {{ ref(source) }}` comment for each configured source model.

      dbts static parser cannot follow refs buried inside a var() lookup. 
      Without these comments, dbt will not schedule source models ahead of the warehouse model, 
      causing run failures when sources do not yet exist in the target schema.
    -#}
    {%- set custom_data_source = var(custom_data_source_model_config, {}) -%}
        {%- if custom_data_source is mapping and custom_data_source|length -%}
            {%- for source_name, _ in custom_data_source.items() -%}
-- depends_on: {{ ref(source_name) }}
            {%- endfor -%}
    {%- endif -%}
{%- endmacro -%}


{%- macro add_custom_data_source_joins(custom_data_sources, driving_alias=none, join_cols=none) -%}
    {#-
        Appends a LEFT JOIN for each configured source. 
        The join style depends on whether the source config includes a `joins` key.

        Original-format config (no `joins` key): builds join conditions from `driving_alias` and
        `join_cols`. Specify one key column per condition.

            ```YAML
            bld_ef3__ell_annual:
            is_english_language_learner_annual:
                where: "coalesce(is_ell_annual, False)"
            ```
            ```SQL
            -- compiles to:
            left join bld_ef3__ell_annual as bld_ef3__ell_annual
                on formatted.k_student = bld_ef3__ell_annual.k_student
            ```

        New-format config (`joins` key present): uses the join expressions from config directly.
        `driving_alias` and `join_cols` are not used.

            ```YAML
            bld_ef3__ell_annual:
            joins:
                - "formatted.k_student = bld_ef3__ell_annual.k_student"
                - "formatted.begin_date = bld_ef3__ell_annual.begin_date"
            ```
            ```SQL
            -- compiles to:
            left join bld_ef3__ell_annual as bld_ef3__ell_annual
                on formatted.k_student = bld_ef3__ell_annual.k_student
                and formatted.begin_date = bld_ef3__ell_annual.begin_date
            ```
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
            {% else %}
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


{%- macro add_custom_data_source(custom_data_source_model_config, join_cols=none, base='formatted') -%}
    {#-
        The single model call for custom data sources in a warehouse model. 
        We call this between the last CTE and the final `select * from ...`.

        Wraps `base` in a `add_custom_data_source` CTE that left-joins any configured sources and adds their columns.
        When no sources are configured, it just generates a `select * from base`.

        Note: the `add_custom_data_source` CTE is necessary to ensure that the custom columns are available in the final select statement, 
        and that the joins are applied before any downstream transformations.
        `select * from add_custom_data_source` still compiles.

        Parameters:
            custom_data_source_model_config : the var key to look up (e.g., 'edu:stu_demos:custom_data_sources').
            join_cols                       : (optional) list of join key columns.
            base                            : (optional) name of the base CTE to wrap; defaults to 'formatted'.

        Example:
            {{ add_custom_data_source('edu:cohort:custom_data_sources', join_cols=['k_cohort']) }}
            select * from add_custom_data_source
    -#}
    {%- set custom_data_source = var(custom_data_source_model_config, {}) -%}
    {{ custom_data_source_depends_on(custom_data_source_model_config) }}
    , add_custom_data_source as (
        select {{ base }}.*
            {{- add_custom_data_source_columns(custom_data_sources=custom_data_source) }}
        from {{ base }}
        {{- add_custom_data_source_joins(custom_data_sources=custom_data_source, driving_alias=base, join_cols=join_cols) }}
    )
{%- endmacro -%}

{%- macro add_custom_data_source_columns(custom_data_sources) -%}
    {#-
      Appends a column expression for each configured source. Two config formats are supported.

      Original format: each indicator name maps to a `where` expression, emitted verbatim as SQL.
      The expression can be a column reference, a function call, or any valid SQL expression.

        ```YAML
        bld_ef3__ell_annual:
          is_english_language_learner_annual:
            where: "coalesce(is_ell_annual, False)"
        ```
        ```SQL
        -- compiles to: 
        , coalesce(is_ell_annual, False) as is_english_language_learner_annual
        ```

      New format: columns are listed under `add_cols` with an alias (`as`) and a
      default value used when the join produces no match.

        ```YAML
        bld_ef3__ell_annual:
          add_cols:
            is_ell_annual: { as: is_english_language_learner_annual, default: false }
        ```
        ```SQL
        -- compiles to: 
        , coalesce(bld_ef3__ell_annual.is_ell_annual, false) as is_english_language_learner_annual
        ```
    -#}

    {% if custom_data_sources is mapping and custom_data_sources|length %}
        {% for source_name, source_config in custom_data_sources|dictsort %}
            {% if 'joins' in source_config %}
                {# New config pathway #}
                {% if 'add_cols' in source_config and source_config.add_cols %}
                    {% for src_col_name, src_col_config in source_config.add_cols.items() %}
                        , coalesce({{ source_name }}.{{ src_col_name }}, {{ src_col_config.default }}) as {{ src_col_config.as }}
                    {% endfor %}
                {% endif %}
            {% else %}
                {# Original config pathway #}
                {% for indicator_name, indicator_config in source_config.items() %}
                    , {{ indicator_config.where }} as {{ indicator_name }}
                {% endfor %}
            {% endif %}
        {% endfor %}
    {% endif %}
{%- endmacro -%}
