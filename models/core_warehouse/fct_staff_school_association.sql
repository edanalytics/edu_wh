{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_staff, k_school, school_year, program_assignment, staff_classification, begin_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

with stg_staff_school as (
    select * from {{ ref('stg_ef3__staff_school_associations') }}
),
stg_staff_ed_org_assign as (
    select * from {{ ref('stg_ef3__staff_education_organization_assignment_associations')}}
),

-- todo:
-- consider assignment semantics:
    -- is this coalesce order right?
    -- how do we want to think about this grain?
        -- consider staff who is both principal and teacher?
-- consider rolling up grade levels and academic subjects into list cols
-- consider calendar reference
  -- may remove if typically unused, unuseful
dim_school as (
    select * from {{ ref('dim_school') }}
),
dim_staff as (
    select * from {{ ref('dim_staff') }}
),
dim_school_calendar as (
    select * from {{ ref('dim_school_calendar') }}
),
formatted as (
    select 
        dim_staff.k_staff,
        dim_school.k_lea,
        dim_school.k_school,
        dim_school_calendar.k_school_calendar,
        stg_staff_school.tenant_code,
        stg_staff_school.school_year,
        stg_staff_school.program_assignment,
        coalesce(school_assign.position_title, 
                 lea_assign.position_title) as position_title,
        coalesce(school_assign.begin_date, 
                 lea_assign.begin_date) as begin_date,
        coalesce(school_assign.end_date, 
                 lea_assign.end_date) as end_date,
        coalesce(school_assign.full_time_equivalency, 
                 lea_assign.full_time_equivalency) as full_time_equivalency,
        coalesce(school_assign.order_of_assignment, 
                 lea_assign.order_of_assignment) as order_of_assignment,
        coalesce(school_assign.staff_classification, 
                 lea_assign.staff_classification) as staff_classification
        {# add any extension columns configured from stg_ef3__staff_school_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__staff_school_associations', flatten=False) }}
        {# add any extension columns configured from stg_ef3__staff_education_organization_assignment_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__staff_education_organization_assignment_associations', flatten=False) }}
    from stg_staff_school
    join dim_school 
        on stg_staff_school.k_school = dim_school.k_school
    join dim_staff 
        on stg_staff_school.k_staff = dim_staff.k_staff
    left join stg_staff_ed_org_assign as lea_assign
        on stg_staff_school.k_staff = lea_assign.k_staff
        and stg_staff_school.school_year = lea_assign.school_year
        and dim_school.k_lea = lea_assign.k_lea
        and lea_assign.ed_org_type = 'LocalEducationAgency'
    left join stg_staff_ed_org_assign as school_assign
        on stg_staff_school.k_staff = school_assign.k_staff
        and stg_staff_school.school_year = school_assign.school_year
        and dim_school.k_school = school_assign.k_school
        and school_assign.ed_org_type = 'School'
    -- staff-calendar association is optional
    left join dim_school_calendar
        on stg_staff_school.k_school_calendar = dim_school_calendar.k_school_calendar
),
check_active as (
    select *,
        -- create indicator for active position
        iff(
            -- is highest school year observed by tenant
            school_year = max(school_year) 
                over(partition by tenant_code)
            -- not yet exited
            and (end_date is null
                or end_date >= current_date())
            -- employment has begun
            and begin_date <= current_date(),
            true, false
        ) as is_active_assignment
    from formatted
)
select * from check_active