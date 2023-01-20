with stg_spec_ed_program_services as (
    select * from {{ ref('stg_ef3__stu_spec_ed__program_services') }}
),
wide as (
    select 
      tenant_code,
      api_year,
      k_student,
      k_student_xyear,
      k_program,
      k_lea,
      k_school,
      spec_ed_program_begin_date,
      array_agg(special_education_program_service) within group (order by special_education_program_service) as special_education_program_services
    from stg_spec_ed_program_services
    {{ dbt_utils.group_by(n=8) }}

)
select * from wide