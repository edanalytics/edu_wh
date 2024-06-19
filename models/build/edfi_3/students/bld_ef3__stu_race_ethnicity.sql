with stg_stu_races as (
    select * from {{ ref('stg_ef3__stu_ed_org__races') }}
),
stg_stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations') }}
),
build_array as (
    select 
        k_student,
        ed_org_id,
        array_agg(race) as race_array
    from stg_stu_races
    group by 1,2
)
select 
    stg_stu_ed_org.tenant_code,
    stg_stu_ed_org.api_year,
    stg_stu_ed_org.k_student_xyear,
    build_array.*,
    -- build single value race_ethnicity
    case 
        when stg_stu_ed_org.has_hispanic_latino_ethnicity
            then '{{ var("edu:stu_demos:hispanic_latino_code") }}'
        when array_size(race_array) > 1
            then '{{ var("edu:stu_demos:multiple_races_code") }}'
        when array_size(race_array) = 1
            then race_array[0]
        else '{{ var("edu:stu_demos:race_unknown_code") }}'
    end as race_ethnicity,
    stg_stu_ed_org.has_hispanic_latino_ethnicity,
    'test' as test
from stg_stu_ed_org
-- this join order is necessary because students with missing race/ethnicity 
--     data are not included in stg_ef3__stu_ed_org__races -> build_array
left join build_array
    on stg_stu_ed_org.k_student = build_array.k_student
    and stg_stu_ed_org.ed_org_id = build_array.ed_org_id
