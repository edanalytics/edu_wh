with stg_student as (
    select * from {{ ref('stg_ef3__students') }}
),
build_object as (
    select 
        tenant_code,
        k_student_xyear,
        k_student,
        api_year
    from stg_student
    qualify api_year = max(api_year) over (partition by k_student_xyear) 
)
select * from build_object