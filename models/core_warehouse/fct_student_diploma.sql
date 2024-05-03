

with stg_diplomas as (
    select * from {{ ref('stg_ef3__student_academic_records__diplomas') }}
),

stg_academic_records as (
      select * from {{ ref('stg_ef3__student_academic_records') }}

), 

joined_diploma as (
    select
        stg_diplomas.tenant_code, 
        stg_diplomas.api_year, 
        stg_academic_records.k_student,
        stg_academic_records.school_year, 
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
    join stg_academic_records on stg_diplomas.k_student_academic_record = stg_academic_records.k_student_academic_record
                              and stg_diplomas.tenant_code = stg_academic_records.tenant_code
                              and stg_diplomas.api_year = stg_academic_records.api_year

),

dedupe_diplomas as (
    {{ 
        dbt_utils.deduplicate(
            relation='joined_diploma',
            partition_by='diploma_type, diploma_award_date',
            order_by = 'diploma_type'
        )
    }}

)
select * from dedupe_diplomas
