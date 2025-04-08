{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_discipline_actions_event, discipline_action)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

with stg_discipline_actions as (
    select * from {{ ref('stg_ef3__discipline_actions') }}
),
bld_discipline_incident_associations as (
    select * from {{ ref('bld_ef3__stu_discipline_action__stu_incident_behavior') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
xwalk_discipline_actions as (
    select * from {{ ref('xwalk_discipline_actions') }}
),
flatten_staff_keys as (
    select 
        k_student,
        k_student_xyear,
        discipline_action_id,
        discipline_date,
        index,
        {{ edu_edfi_source.gen_skey('k_staff', alt_ref='value:staffReference') }}
    from stg_discipline_actions
        {{ edu_edfi_source.json_flatten('v_staffs') }}
),
agg_staff_keys as (
    select 
        k_student,
        k_student_xyear,
        discipline_action_id,
        discipline_date,
        -- staff associations are most often singular.
        -- keep the first such association, but also produce an array in case of multiple
        max(case when index = 0 then k_staff else null end) as k_staff_single,
        array_agg(k_staff) as k_staff_array
    from flatten_staff_keys
    group by 1,2,3,4
),
formatted as (
    -- this introduces a new grain: by action id and discipline type
    -- in most cases this doesn't actually change the grain, but it could
    select 
        dim_student.k_student,
        dim_student.k_student_xyear,
        coalesce(
            dim_school__responsibility.k_school,
            dim_school__assignment.k_school) as k_school,
        dim_school__assignment.k_school as k_school__assignment,
        dim_school__responsibility.k_school as k_school__responsibility,
        stg_discipline_actions.school_year,
        {{ dbt_utils.generate_surrogate_key(
            [
                'stg_discipline_actions.tenant_code',
                'stg_discipline_actions.api_year',
                'stg_discipline_actions.discipline_action_id',
                'stg_discipline_actions.discipline_date'
            ]
        ) }} as k_discipline_actions_event,
        agg_staff_keys.k_staff_single as k_staff,
        stg_discipline_actions.tenant_code,
        stg_discipline_actions.discipline_action_id,
        stg_discipline_actions.discipline_date,
        {{ edu_edfi_source.extract_descriptor('value:disciplineDescriptor::string') }} as discipline_action,
        stg_discipline_actions.discipline_action_length,
        stg_discipline_actions.actual_discipline_action_length,
        stg_discipline_actions.triggered_iep_placement_meeting,
        stg_discipline_actions.is_related_to_zero_tolerance_policy,
        stg_discipline_actions.discipline_action_length_difference_reason,
        agg_staff_keys.k_staff_array,
        bld_discipline_incident_associations.k_student_discipline_incident_behavior_array
        {# add any extension columns configured from stg_ef3__discipline_actions #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__discipline_actions', flatten=False) }}
    from stg_discipline_actions
    join dim_student 
        on stg_discipline_actions.k_student = dim_student.k_student
    -- not all discipline actions have an incident association, want to keep those records as well
    left join bld_discipline_incident_associations
        on stg_discipline_actions.k_student = bld_discipline_incident_associations.k_student
        and stg_discipline_actions.k_student_xyear = bld_discipline_incident_associations.k_student_xyear
        and stg_discipline_actions.discipline_action_id = bld_discipline_incident_associations.discipline_action_id
        and stg_discipline_actions.discipline_date = bld_discipline_incident_associations.discipline_date
    left join dim_school as dim_school__assignment
        on stg_discipline_actions.k_school__assignment = dim_school__assignment.k_school
    left join dim_school as dim_school__responsibility
        on stg_discipline_actions.k_school__responsibility = dim_school__responsibility.k_school
    left join agg_staff_keys
        on stg_discipline_actions.k_student = agg_staff_keys.k_student
        and stg_discipline_actions.discipline_action_id = agg_staff_keys.discipline_action_id
        and stg_discipline_actions.discipline_date = agg_staff_keys.discipline_date
    {{ edu_edfi_source.json_flatten('v_disciplines') }}
    -- brule: one or the other school must be populated
    where (assignment_school_id is not null or responsibility_school_id is not null)
),
join_descriptor_interpretation as (
    select 
        formatted.*,
        xwalk_discipline_actions.is_oss,
        xwalk_discipline_actions.is_iss,
        xwalk_discipline_actions.is_exp,
        xwalk_discipline_actions.is_minor,
        -- bring in any additional custom columns from xwalk, does nothing if there are no extra columns
        {{ accordion_columns('xwalk_discipline_actions',
            exclude_columns=['discipline_action','severity_order','is_oss','is_iss','is_exp','is_minor']) }}
        xwalk_discipline_actions.severity_order,
        -- for a specific discipline event (which can include multiple disciplines)
        -- flag the most severe discipline
        -- this will also handle if there are ties in severity and just choose the first option
        case
            when xwalk_discipline_actions.severity_order is null
                then null
            when xwalk_discipline_actions.severity_order is not null 
                and 1 = row_number() over (partition by k_student, k_student_xyear, k_school, k_discipline_actions_event 
                                           order by xwalk_discipline_actions.severity_order desc nulls last)
                then true
            else false
        end as is_most_severe_action
    from formatted
    left join xwalk_discipline_actions
        on formatted.discipline_action = xwalk_discipline_actions.discipline_action
)
select * from join_descriptor_interpretation
