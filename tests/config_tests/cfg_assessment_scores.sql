{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}

{# List all xwalk column names #}
{%- set xwalk_column_names = adapter.get_columns_in_relation(ref('xwalk_assessment_scores')) | map(attribute='column') | map('lower') | list-%}

with fct_student_assessment  as (
    select * from {{ ref('fct_student_assessment') }}
),

dim_assessment as (
    select * from {{ ref('dim_assessment') }}
),

xwalk_assessment_scores as (
    select * from {{ ref('xwalk_assessment_scores') }}
),

joined as (
    select distinct
        fct_student_assessment.tenant_code, 
        fct_student_assessment.school_year,
        dim_assessment.assessment_identifier,
        dim_assessment.assessment_family
    from fct_student_assessment
    join dim_assessment 
        on fct_student_assessment.k_assessment = 
            dim_assessment.k_assessment
    {# Join on assessment_family if it is present in the xwalk. Else, join on assessment identifier only. #}
    {% if 'assessment_family' in xwalk_column_names %}
    left join xwalk_assessment_scores
        on ifnull(xwalk_assessment_scores.assessment_identifier, '1') = iff(xwalk_assessment_scores.assessment_identifier is null, '1', dim_assessment.assessment_identifier)
        and ifnull(xwalk_assessment_scores.assessment_family, '1') = iff(xwalk_assessment_scores.assessment_family is null, '1', dim_assessment.assessment_family)
    where 
        xwalk_assessment_scores.assessment_identifier is null 
        and xwalk_assessment_scores.assessment_family is null
    {% else %}
    left join xwalk_assessment_scores
        on dim_assessment.assessment_identifier = xwalk_assessment_scores.assessment_identifier
    where 
        xwalk_assessment_scores.assessment_identifier is null 
    {% endif %}
    order by assessment_identifier
)

select count(*) as failed_row_count, tenant_code, school_year from joined
group by all
having count(*) > 1