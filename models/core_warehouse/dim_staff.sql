{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_staff)",
    ]
  )
}}

with stg_staff as (
    select * from {{ ref('stg_ef3__staffs') }}
),
bld_ef3__wide_ids_staff as (
    select * from {{ ref('bld_ef3__wide_ids_staff') }}
),
choose_email as (
    {{ row_pluck(ref('stg_ef3__staffs__emails'),
                key='k_staff',
                column='email_type',
                preferred=var('edu:staff:preferred_email', 'Work'),
                where='(do_not_publish is null or not do_not_publish)') }}
),
-- emails
formatted as (
    select 
        stg_staff.k_staff,
        stg_staff.tenant_code,
        stg_staff.staff_unique_id,
        {{ accordion_columns(
            source_table='bld_ef3__wide_ids_staff', 
            exclude_columns=['tenant_code', 'api_year', 'k_staff']) }}
        stg_staff.login_id,
        choose_email.email_address,
        choose_email.email_type,
        stg_staff.last_name || ', ' || stg_staff.first_name as display_name,
        stg_staff.first_name,
        stg_staff.last_name,
        stg_staff.middle_name,
        stg_staff.personal_title_prefix,
        stg_staff.generation_code_suffix,
        stg_staff.birth_date,
        stg_staff.gender,
        stg_staff.highest_completed_level_of_education,
        stg_staff.is_highly_qualified_teacher,
        stg_staff.years_of_prior_professional_experience,
        stg_staff.years_of_prior_teaching_experience
    from stg_staff
    left join bld_ef3__wide_ids_staff 
        on stg_staff.k_staff = bld_ef3__wide_ids_staff.k_staff
    left join choose_email
        on stg_staff.k_staff = choose_email.k_staff
)
select * from formatted
order by tenant_code, k_staff
