

with stg_diplomas as (
    select * from {{ ref('stg_ef3__student_academic_records__diplomas') }}
),

stu_academic_records as (
      select * from {{ ref('fct_student_academic_record') }}

), 

joined_diploma as (
    select
        stg_diplomas.tenant_code,  
        stu_academic_records.k_student,
        stu_academic_records.k_student_xyear,
        stu_academic_records.k_lea, 
        stu_academic_records.k_school, 
        stu_academic_records.school_year, 
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
    from stg_diplomas 
    join stu_academic_records on stg_diplomas.k_student_academic_record = stu_academic_records.k_student_academic_record

),

dedupe_diplomas as (
    {{ 
        dbt_utils.deduplicate(
            relation='joined_diploma',
            partition_by='k_student, k_student_xyear, school_year, k_lea, k_school, diploma_type, diploma_award_date',
            order_by = 'diploma_type'
        )
    }}

)
select * from dedupe_diplomas
