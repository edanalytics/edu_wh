with stg_course_transcripts as (
    select * from {{ ref('stg_ef3__course_transcripts') }}
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
    from stg_course_transcripts
    qualify api_year = max(api_year) over (
        partition by
            tenant_code,
            lower(student_unique_id),
            school_year
    )
)
select * from latest_ods
