{%- set name_type_list = ['personalTitlePrefix', 'firstName', 'middleName', 'lastSurname', 'generationCodeSuffix']-%}

with stg_other_names as (
    select * from {{ ref('stg_ef3__students__other_names') }}
),
widened as (
    select  
        * exclude (otherNameType, personalTitlePrefix, firstName, middleName, lastSurname, generationCodeSuffix),
        {%- for name_type in name_type_list -%}
            {{ ea_pivot(
                    column='otherNameType',
                    values=dbt_utils.get_column_values(ref('stg_ef3__students__other_names'),'otherNameType'),
                    agg='min',
                    suffix='_' ~ dbt_utils.slugify(name_type),
                    then_value=name_type,
                    else_value='null',
            )}}
            {%- if not loop.last -%},{%- endif-%}   
         {%- endfor-%}
    from stg_other_names
    group by all

)
select * from widened