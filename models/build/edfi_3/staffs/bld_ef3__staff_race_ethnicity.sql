with stg_staff_races as (
    select * from {{ ref('stg_ef3__staffs__races') }}
),
stg_staffs as (
    select * from {{ ref('stg_ef3__staffs') }}
),
build_array as (
    select 
        k_staff,
        array_agg(race) as race_array
    from stg_staff_races
    group by 1
)
select 
    stg_staffs.tenant_code,
    stg_staffs.api_year,
    stg_staffs.k_staff,
    build_array.race_array,
    -- build single value race_ethnicity
    case 
        when stg_staffs.has_hispanic_latino_ethnicity
            {# default to stu var if staff var not defined #}
            then '{{ var("edu:staff_demos:hispanic_latino_code", var("edu:stu_demos:hispanic_latino_code")) }}'
        when {{ edu_edfi_source.json_array_size('race_array') }} > 1
            {# default to stu var if staff var not defined #}
            then '{{ var("edu:staff_demos:multiple_races_code", var("edu:stu_demos:multiple_races_code")) }}'
        when {{ edu_edfi_source.json_array_size('race_array') }} = 1
            then race_array[0]
            {# default to stu var if staff var not defined #}
        else '{{ var("edu:staff_demos:race_unknown_code", var("edu:stu_demos:race_unknown_code")) }}'
    end as race_ethnicity
from build_array
join stg_staffs
    on build_array.k_staff = stg_staffs.k_staff
