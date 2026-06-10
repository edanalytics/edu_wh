{#-
    Custom data sources let stadium repos append columns from their own dbt models to core
    warehouse tables without modifying this package.

    How it works:
        1. Add a source config in dbt_project.yml under `edu:custom_data_sources`, keyed by the warehouse model name.
        2. Each warehouse model declares an `add_custom_data_source` CTE between its last CTE and its final
           SELECT, calling the macro inside it to append the SELECT body.
        3. The macro appends the SELECT body that left-joins each configured source and appends its columns.
           If nothing is configured, it passes through with a simple `select * from base`.

    Config format:
        ```YAML
        vars:
            edu:custom_data_sources:
                dim_student:                 # name of a dbt model in the edu_wh repo you would like to append CDS to.
                    bld_ef3__ell_annual:     # name of a dbt model in the stadium implementation with CDS.
                        add_cols:            # columns to add to the core model (dim_student) from the CDS source (bld_ef3__ell_annual).
                            is_ell_annual:   # name of the column in the CDS source model (bld_ef3__ell_annual.is_ell_annual) to add to the core model.
                                as: is_english_language_learner_annual  # alias for the column in the final SELECT output (dim_student.is_english_language_learner_annual).
                                default: false   # default value to use when there is no match in the source model (i.e. the LEFT JOIN fails). Optional, defaults to False.
        ```

        By default, the source is joined on the models primary key as declared in its schema YAML constraints.
        Specify `joins` in the source config to override with custom conditions:

        ```YAML
        vars:
            edu:custom_data_sources:
                fct_student_school_association:
                    bld_ef3__cds_enrollment:
                        joins:
                            - "formatted.k_student = my_source.k_student"
                            - "formatted.entry_date >= my_source.begin_date"
                        add_cols:
                            my_col:
                                as: my_col_alias
                                default: null
        ```

    Model usage:

        ```SQL
        --- edu_wh model SQL file with logic.
        ...
        formatted as (...SQL...),

        -- add the custom data source CTE.
        add_custom_data_source as (
            {{ add_custom_data_source(relation='formatted') }}
        )
        select * from add_custom_data_source

        ```
        ```SQL
        -- pass the name of whichever CTE immediately precedes this one:
        add_custom_data_source as (
            {{ add_custom_data_source(relation='dedupe_assessments') }}
        )
        select * from add_custom_data_source
        ```

    Macros in this file:
        add_custom_data_source         : appends the SELECT body for the add_custom_data_source CTE.
        add_custom_data_source_joins   : appends LEFT JOIN clauses.
        add_custom_data_source_columns : appends column expressions.
        custom_data_source_depends_on  : appends depends_on comments for dbts static parser.
-#}


{%- macro add_custom_data_source(relation) -%}
    {#-
        Appends the SELECT body for the `add_custom_data_source` CTE declared in each warehouse model.

        Left-joins any configured sources and appends their columns to `SELECT *` from the relation CTE.
        When no sources are configured, appends a simple `select * from relation`.

        Reads the current models primary key from the `primary_key` constraint in its schema YAML and
        uses those columns as the default join condition. 
        Individual sources can override this with a `joins` key in their config.

        Parameters:
            relation: name of the base CTE (last CTE in the model) to select from (e.g. 'formatted').

        Example:
            ...
            formatted as (
                select * from model
            ),
            add_custom_data_source as (
                {{ add_custom_data_source(relation='formatted') }}
            )
            select * from add_custom_data_source
    -#}
    {%- set model_name = model.name -%}

    {#- This finds the constraint in the models yaml that contains a type of `primary_key` -#}
    {%- set pk_constraint = model.constraints | selectattr('type', 'equalto', 'primary_key') | list | first-%}
    
    {#- We use the primary keys defined in that contraint section as the default columns to use in our CDS joins -#}
    {%- set default_join_cols = pk_constraint.columns if pk_constraint else [] %}
    {%- set all_cds = var('edu:custom_data_sources', {}) %}
    {%- set custom_data_source = all_cds[model_name] if all_cds and model_name in all_cds else {} %}
    
    {#- This is the SQL we are appending in the add_custom_data_source CTE -#}
    {{ custom_data_source_depends_on(model_name) }}
    select 
        {{ relation }}.*
        {{ add_custom_data_source_columns(custom_data_sources=custom_data_source) }}
    from {{ relation }}
    {{ add_custom_data_source_joins(
        custom_data_sources=custom_data_source,
        default_join_cols=default_join_cols,
        driving_alias=relation
    ) }}
{%- endmacro -%}


{%- macro custom_data_source_depends_on(model_name) -%}
    {#-
      Appends a `-- depends_on: {{ ref(source) }}` comment for each configured source model.

      dbts static parser cannot follow refs buried inside a var() lookup.
      Without these comments, dbt will not schedule source models ahead of the warehouse model,
      causing run failures when sources do not yet exist in the target schema.
    #}
    {%- set all_cds = var('edu:custom_data_sources', {}) %}
    {%- set custom_data_source = all_cds[model_name] if all_cds and model_name in all_cds else {} %}
        {%- if custom_data_source is mapping and custom_data_source|length %}
            {%- for source_name, _ in custom_data_source.items() %}
    -- depends_on: {{ ref(source_name) }}
            {%- endfor %}
    {%- endif %}
{%- endmacro -%}


{%- macro add_custom_data_source_columns(custom_data_sources) -%}
    {#-
        Appends a column expression for each configured source. Columns are listed under
        `add_cols` with an alias (`as`) and a default value for unmatched rows.

            ```YAML
            bld_ef3__ell_annual:
                add_cols:
                    is_ell_annual:
                        as: is_english_language_learner_annual
                        default: false
            ```
            ```SQL
            -- compiles to:
            coalesce(bld_ef3__ell_annual.is_ell_annual, false) as is_english_language_learner_annual,
            ```
    -#}
    {%- if custom_data_sources is mapping and custom_data_sources|length %}
        {%- for source_name, source_config in custom_data_sources|dictsort %}
            {%- if 'add_cols' in source_config and source_config.add_cols %}
                {%- for src_col_name, src_col_config in source_config.add_cols.items() %}
                    {%- if loop.first -%},{% endif %}
                    {%- set col_default = src_col_config.default if 'default' in src_col_config else false %}
        coalesce({{ source_name }}.{{ src_col_name }}, {{ 'null' if col_default is none else col_default }}) as {{ src_col_config.as }}{% if not loop.last %},{% endif %}
                {%- endfor %}
            {%- endif %}
        {%- endfor %}
    {%- endif %}
{%- endmacro -%}


{%- macro add_custom_data_source_joins(custom_data_sources, default_join_cols=none, driving_alias='formatted') -%}
    {#-
        Appends a LEFT JOIN for each configured source.

        By default, joins on the models primary key columns (read from schema YAML constraints and
        passed in as `default_join_cols`). Specify `joins` in the source config to override.

        Default join (no `joins` key in config):
            ```YAML
            bld_ef3__ell_annual:
                add_cols:
                    is_ell_annual:
                        as: is_english_language_learner_annual
                        default: false
            ```
            ```SQL
            -- compiles to (primary key: [k_student], relation: 'formatted'):
            left join bld_ef3__ell_annual as bld_ef3__ell_annual
                on formatted.k_student = bld_ef3__ell_annual.k_student
            ```

        Custom join (`joins` key present):
            ```YAML
            my_source:
                joins:
                    - "formatted.k_student = my_source.k_student"
                    - "formatted.entry_date >= my_source.begin_date"
            ```
            ```SQL
            -- compiles to:
            left join my_source as my_source
                on formatted.k_student = my_source.k_student
                and formatted.entry_date >= my_source.begin_date
            ```
    #}

    {%- if custom_data_sources is mapping and custom_data_sources|length %}
        {%- for source_name, source_config in custom_data_sources|dictsort %}
        left join {{ ref(source_name) }} as {{ source_name }}
            {%- if 'joins' in source_config and source_config.joins %}
                {%- for join in source_config.joins %}
                    {%- if loop.first %}
            on {{ join }}
                    {%- else %}
            and {{ join }}
                    {%- endif %}
                {%- endfor %}
            {%- elif default_join_cols %}
                {%- for col in default_join_cols %}
                    {%- if loop.first %}
            on {{ driving_alias }}.{{ col }} = {{ source_name }}.{{ col }}
                    {%- else %}
            and {{ driving_alias }}.{{ col }} = {{ source_name }}.{{ col }}
                    {%- endif %}
                {%- endfor %}
            {%- endif %}
        {%- endfor %}
    {%- endif %}
{%- endmacro -%}