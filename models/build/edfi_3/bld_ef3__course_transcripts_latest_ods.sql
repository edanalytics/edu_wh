with stg_course_transcripts as (
    select * from {{ ref('stg_ef3__course_transcripts') }}
),

dim_school as (
    select * from {{ ref('dim_school') }}
),

dim_lea as (
    select * from {{ ref('dim_lea') }}
),

-- The transcript's academic record reference only has an ed org id, not whether it is a school or an LEA.
-- Ed-Fi ed org ids are unique across ed org types, so at most one of these joins matches.
add_keys as (
    select
        stg_course_transcripts.*,
        dim_school.k_school,
        coalesce(dim_lea.k_lea, dim_school.k_lea) as k_lea  -- fill district if record is specified at school level
    from stg_course_transcripts
    left join dim_school
        on stg_course_transcripts.tenant_code = dim_school.tenant_code
        and stg_course_transcripts.student_academic_record_ed_org_id = dim_school.school_id
    left join dim_lea
        on stg_course_transcripts.tenant_code = dim_lea.tenant_code
        and stg_course_transcripts.student_academic_record_ed_org_id = dim_lea.lea_id
),

-- A newer ODS can have a student's academic record without any of their courses.
-- So we choose where to get transcripts separately from where we get academic records.
-- For example:
    -- Student | API Year | Has Academic Record | Transcript Lines | Use Transcripts?
    -- --------|----------|---------------------|------------------|-----------------
    -- 123     | 2026     | True                | 7                | Yes
    -- 123     | 2027     | True                | 0                | No

-- Because the 2027 ODS has no transcript lines, we fall back to the 2026 ODS for transcripts.

-- For each student and school year, take all transcripts from the newest ODS that has any transcript lines.
-- This only falls back when the newer ODS has no transcript lines.
-- If it has some courses but leaves others out, we still take the newer set.

latest_ods as (
    select *
    from add_keys
    qualify api_year = max(api_year) over (
        partition by
            k_student_xyear,
            school_year
    )
)
select * from latest_ods
