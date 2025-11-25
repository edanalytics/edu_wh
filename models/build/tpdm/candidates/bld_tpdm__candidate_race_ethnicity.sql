with stg_candidate_races as (
    select * from {{ ref('stg_tpdm__candidate_races') }}
),
stg_candidates as (
    select * from {{ ref('stg_tpdm__candidates') }}
),
build_array as (
    select 
        k_candidate,
        array_agg(race) as race_array
    from stg_candidate_races
    group by 1
)
select 
    stg_candidates.k_candidate,
    stg_candidates.tenant_code,
    stg_candidates.school_year,
    stg_candidates.k_candidate_xyear,
    build_array.race_array,
    -- build single value race_ethnicity
    case 
        when stg_candidates.has_hispanic_latino_ethnicity
            then '{{ var("edu:stu_demos:hispanic_latino_code") }}'
        when array_size(race_array) > 1
            then '{{ var("edu:stu_demos:multiple_races_code") }}'
        when array_size(race_array) = 1
            then race_array[0]
        else '{{ var("edu:stu_demos:race_unknown_code") }}'
    end as race_ethnicity,
    stg_candidates.has_hispanic_latino_ethnicity
from stg_candidates
-- this join order is necessary because students with missing race/ethnicity 
--     data are not included in stg_ef3__stu_ed_org__races -> build_array
left join build_array
    on stg_candidates.k_candidate = build_array.k_candidate
