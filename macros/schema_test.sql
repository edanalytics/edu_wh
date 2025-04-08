{% test relationship(model, parent, column_name) %} 
    {%- set all_columns = dbt_utils.get_filtered_columns_in_relation(model) %}

    with child as (
        select {{ column_name }} as from_field,
            {%-if 'TENANT_CODE' in all_columns %}
                tenant_code,
            {%- endif %}
            {%- if 'API_YEAR' in all_columns %}
                api_year,
            {%- endif %}
        from {{ model }}
        where {{ column_name  }} is not null
    ),
    parent as (
        select {{ column_name }} as to_field,
            {%-if 'TENANT_CODE' in all_columns %}
                tenant_code,
            {%- endif %}
            {%- if 'API_YEAR' in all_columns %}
                api_year,
            {%- endif %}
        from {{ ref(parent) }}
    )
    
    select
        child.tenant_code,
        child.api_year,
        '{{ parent }}' as parent_model_name,
        object_construct('test_column', array_construct('{{ column_name }}') )  as test_params,
        count(*) as failed_row_count
    from child
    left join parent
        on child.from_field = parent.to_field
    where parent.to_field is null
    group by all
{% endtest %}



{% test unique_combination_of_columns(model, combination_of_columns) %}
    {%- set all_columns = adapter.get_columns_in_relation(model) %}

    {%- set column_list=combination_of_columns  %}
    {%- set columns_csv=column_list | join(', ') %}


    with validation_errors as (
        select
            {{ columns_csv }},
        {%-if 'TENANT_CODE' in all_columns %}
            tenant_code,
        {%- endif %}
        {%- if 'API_YEAR' in all_columns %}
            api_year,
        {%- endif %}
        {%- if 'SCHOOL_YEAR' in all_columns %}
            school_year,
        {%- endif %}
            count(*) as failed_row_count
        from {{ model }}
        group by all
        having count(*) > 1

    )

    select distinct
        {%-if 'TENANT_CODE' in all_columns %}
            tenant_code,
        {%- endif %}
        {%- if 'API_YEAR' in all_columns %}
            api_year,
        {%- endif %}
        {%- if 'SCHOOL_YEAR' in all_columns %}
            school_year,
        {%- endif %}
            failed_row_count,
        object_construct('test_columns', array_construct({{columns_csv}}) )  as test_params
    from validation_errors

{% endtest %}


{% test accepted_values(model, values, column_name, quote = true) %}
    {%- set all_columns = adapter.get_columns_in_relation(model) %}
    {%- if quote %}
        {%- set accepted_list = [] %}
        {%- for value in values %}
            {%- if value is string and value[0] != "'" and value[-1] != "'" %}
                {%- set accepted_list = accepted_list.append("'" + value + "'") %}
            {%- else %}
                {%- set accepted_list = accepted_list.append(value) %}
            {%- endif %}
        {%- endfor %}
    {%- elif not quote %}
        {%- set accepted_list = values %}
    {%- endif%}

    select 
        tenant_code,
        {%- if 'API_YEAR' in all_columns %}
        api_year,
        {%- endif %}
        count(*) as failed_row_count,
        object_construct('accepted_values', {{values}} ) as test_params
    from {{ model }}
    where {{ column_name }} not in ( {%- for value in accepted_list %} {{value}} {%- if not loop.last %}, {%-else %} {%-endif%} {%-endfor%} )
    group by all

{% endtest%}


{% test not_null(model, column_name) %}
    {%- set all_columns = adapter.get_columns_in_relation(model) %}

    select 
        {%-if 'TENANT_CODE' in all_columns %}
        tenant_code,
        {%- endif %}
        {%- if 'API_YEAR' in all_columns %}
        api_year,
        {%- endif %}
        count(*) as failed_row_count,
        object_construct('test_column', array_construct('{{ column_name }}') ) )as test_params
    from {{ model }}
    where {{ column_name }} is null
    group by all

{% endtest %}


{% test unique(model, column_name) %}
     {%- set all_columns = adapter.get_columns_in_relation(model) %}

    with validation_errors as (
        select 
            column_name
        from {{ model }}
        where {{ column_name }} is not null
        group by all
        having count(*) > 1
    )

    select 
        {%-if 'TENANT_CODE' in all_columns %}
        tenant_code,
        {%- endif %}
        {%- if 'API_YEAR' in all_columns %}
        api_year,
        {%- endif %}
        count(*) as failed_row_count,
        object_construct('test_column', array_construct('{{ column_name }}') )  as test_params
    from validation_errors

{% endtest %}




