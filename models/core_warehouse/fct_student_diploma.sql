{% set xwalk_academic_term_enabled = var('edu:xwalk_academic_terms:enabled', False) %}

with stg_diplomas as (
    select * from {{ ref('stg_ef3__student_academic_records__diplomas') }}
),
stu_academic_records as (
      select * from {{ ref('fct_student_academic_record') }}
), 
joined_diploma as (
    select
        stg_diplomas.tenant_code,  
        -- we pull k_student, k_student_xyear from stu_academic record to ensure we are 
        -- including historical student records from years prior where the student is not in dim_student. 
        -- This logic is already implemented in fct_student_academic_record, we pull this in to avoid 
        -- duplicating the logic.
        stu_academic_records.k_student,
        stu_academic_records.k_student_xyear,
        stu_academic_records.k_lea, 
        stu_academic_records.k_school, 
        stu_academic_records.school_year, 
        stu_academic_records.academic_term,
        diploma_type, 
        diploma_award_date, 
        diploma_description,
        diploma_level_descriptor, 
        achievement_category_descriptor, 
        achievement_category_system, 
        achievement_title, 
        criteria, 
        criteria_url, 
        is_cte_completer, 
        diploma_award_expires_date, 
        evidence_statement, 
        image_url, 
        issuer_name, 
        issuer_origin_url
        {# add any extension columns configured from stg_ef3__student_academic_records__diplomas #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_academic_records__diplomas', flatten=False) }}
    from stg_diplomas 
    join stu_academic_records 
        on stg_diplomas.k_student_academic_record = stu_academic_records.k_student_academic_record
),
{% if xwalk_academic_term_enabled %}
xwalk_academic_terms as (
    select * from {{ ref('xwalk_academic_terms') }}
),
{% endif %}
joined_with_xwalk as (
    select
        joined_diploma.*,
        {% if xwalk_academic_term_enabled %} 
        xwalk_academic_terms.sort_index
        {% else %}
        NULL as sort_index
        {% endif %}
    from joined_diploma
    {% if xwalk_academic_term_enabled %}
    left join xwalk_academic_terms
        on joined_diploma.academic_term = xwalk_academic_terms.academic_term
    {% endif %}
),
dedupe_diplomas as (
     {{
            dbt_utils.deduplicate(
                relation='joined_with_xwalk',
                partition_by='k_student, k_student_xyear, school_year, k_lea, k_school, diploma_type, diploma_award_date',
                order_by = "sort_index nulls last" if xwalk_academic_term_enabled else "academic_term"
            )
        }}
)
select dedupe_diplomas.* except(academic_term) from dedupe_diplomas
order by tenant_code, k_student
