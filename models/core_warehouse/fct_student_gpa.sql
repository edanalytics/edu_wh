-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student_gpa set not null",
        "alter table {{ this }} add primary key (k_student_gpa)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
    ]
  )
}}

-- A student's GPA may come from an ODS older than their selected academic record.
-- For example, the 2025 ODS may have their 2025 GPA while the 2026 copy leaves it empty.
-- Keep the GPA without requiring a match to the selected academic record's key.

-- To find academic records for the same student and school year, join on k_student_xyear and school_year.
-- This can match more than one academic record.
-- Include academic_term for term-level matches, keeping in mind that terms can change between ODS copies.
with gpas as (
    select * from {{ ref('bld_ef3__gpas_latest_ods') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
formatted as (
    select
        -- Give each GPA its own key using tenant, student, school year, ed org, term, and GPA type.
        -- Leave out api_year so receiving the same GPA in a newer ODS doesn't change its key.
        {{ dbt_utils.generate_surrogate_key(
            [
                'gpas.tenant_code',
                'gpas.school_year',
                'lower(gpas.student_unique_id)',
                'gpas.ed_org_id',
                'lower(gpas.academic_term)',
                'gpas.gpa_type'
            ]
        ) }} as k_student_gpa,
        dim_student.k_student,
        gpas.k_student_xyear,
        coalesce(gpas.k_lea, dim_school.k_lea) as k_lea,
        gpas.k_school,
        gpas.tenant_code,
        gpas.school_year,
        gpas.academic_term,
        gpas.gpa_type,
        gpas.gpa_value,
        gpas.is_cumulative,
        gpas.max_gpa_value
    from gpas
    left join dim_school
        on gpas.k_school = dim_school.k_school
    left join dim_student
        on gpas.k_student_xyear = dim_student.k_student_xyear
        and gpas.school_year = dim_student.school_year
)
select * from formatted
order by tenant_code, k_student
