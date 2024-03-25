{# configure address preferred #}
{% set preferred = 'Physical' %}


with stu_phone_wide as (
    select * from {{ ref('bld_ef3__student_wide_phone_numbers') }}
),
stu_emails_wide as (
    select * from {{ ref('bld_ef3__student_wide_emails') }}
),
stu_address_wide as (
    select * from {{ ref('bld_ef3__student_wide_addresses') }}
),
stu_language_wide as (
    select * from {{ ref('bld_ef3__student_wide_languages') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
), 
choose_address as (
    {{ row_pluck(ref('stg_ef3__stu_ed_org__addresses'),
                key='k_student',
                column='address_type',
                preferred=preferred,
                where='address_end_date is null') }}
)
select 
    dim_student.k_student, 
    dim_student.k_student_xyear,
    dim_student.tenant_code, 
    dim_student.school_year,
    {{ accordion_columns(
            source_table='bld_ef3__student_wide_phone_numbers',
            exclude_columns=["k_student", "tenant_code"],
            source_alias='stu_phone_wide'
        ) }}
    {{ accordion_columns(
            source_table='bld_ef3__student_wide_emails',
            exclude_columns=["k_student", "tenant_code"],
            source_alias='stu_emails_wide'
    ) }} 
    {{ accordion_columns(
            source_table='bld_ef3__student_wide_languages',
            exclude_columns=["k_student", "tenant_code"],
            source_alias='stu_language_wide'
    ) }} 
    {{ accordion_columns(
            source_table='bld_ef3__student_wide_addresses',
            exclude_columns=["k_student", "tenant_code"],
            source_alias='stu_address_wide'
    ) }}
    choose_address.city                   as {{preferred}}_address_city,
    choose_address.name_of_county         as {{preferred}}_address_name_of_county,
    choose_address.state_code             as {{preferred}}_address_state_code,
    choose_address.postal_code            as {{preferred}}_address_postal_code,
    choose_address.building_site_number   as {{preferred}}_address_building_site_number,
    choose_address.locale                 as {{preferred}}_address_locale,
    choose_address.congressional_district as {{preferred}}_address_congressional_district,
    choose_address.county_fips_code       as {{preferred}}_address_county_fips_code,
    choose_address.latitude               as {{preferred}}_address_latitude,
    choose_address.longitude              as {{preferred}}_address_longitude
from dim_student 
left join stu_phone_wide    on dim_student.k_student = stu_phone_wide.k_student 
left join stu_emails_wide   on dim_student.k_student = stu_emails_wide.k_student 
left join stu_language_wide on dim_student.k_student = stu_language_wide.k_student
left join stu_address_wide  on dim_student.k_student = stu_address_wide.k_student
left join choose_address    on stu_address_wide.k_student = choose_address.k_student