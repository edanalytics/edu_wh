{# otherName properties pulled from Ed-Fi Data Handbook (v5.0.0)
https://edfidocs.blob.core.windows.net/$web/handbook/v5.0/index.html#/OtherName82adcecf-8e39-4f24-a5a6-3c32964693c3 #}
{%- set name_type_list = ['personal_title_prefix', 'first_name', 'middle_name', 'last_surname', 'generation_code_suffix']-%}

with stg_other_names as (
    select * from {{ ref('stg_tpdm__candidates__other_names') }}
),
widened as (
    select  
        tenant_code,
        api_year,
        k_candidate,
        k_candidate_xyear
        {%- if not is_empty_model('stg_tpdm__candidates__other_names') -%},
            {%- for name_type in name_type_list -%}
                {{ ea_pivot(
                        column='other_name_type',
                        values=dbt_utils.get_column_values(ref('stg_tpdm__candidates__other_names'),'other_name_type'),
                        agg='min',
                        suffix='_' ~ name_type,
                        then_value=name_type,
                        else_value='null',
                )}}
                {%- if not loop.last -%},{%- endif-%}   
            {%- endfor-%}
        {%- endif %}   
    from stg_other_names
    group by all

)
select * from widened