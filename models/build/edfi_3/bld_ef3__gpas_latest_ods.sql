with combined_gpas as (
    select * from {{ ref('bld_ef3__combine_gpas') }}
    -- The combined model creates rows for the older GPA fields even when they're empty.
    -- Remove those rows so an ODS without GPA values can't be selected.
    where gpa_value is not null
),

stg_academic_record as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),

-- A newer ODS can have a student's academic record and courses but leave out their GPA.
-- So we choose where to get GPAs separately from academic records and transcripts.
-- For example:
    -- Student | API Year | Has Academic Record | GPA | Use GPAs?
    -- --------|----------|---------------------|-----|----------
    -- 123     | 2024     | True                | 3.4 | Yes
    -- 123     | 2025     | True                | None| No
    -- 123     | 2026     | True                | None| No

-- We keep the GPA from the 2024 ODS, even if the academic record or courses come from 2026.

-- For each student and school year, take all GPAs from the newest ODS that has at least one GPA value.
-- Empty GPA rows are removed before making this choice.
-- If a newer ODS includes GPAs, we use that set instead of the older one.

-- We don't choose separately by ed org or term, since those can change between ODS copies.
-- Otherwise, an older GPA could survive alongside its recoded replacement.
gpas_with_record as (
    select
        stg_academic_record.tenant_code,
        combined_gpas.api_year,
        stg_academic_record.school_year,
        stg_academic_record.student_unique_id,
        stg_academic_record.ed_org_id,
        stg_academic_record.academic_term,
        stg_academic_record.k_student_xyear,
        stg_academic_record.k_lea,
        stg_academic_record.k_school,
        combined_gpas.gpa_type,
        combined_gpas.gpa_value,
        combined_gpas.is_cumulative,
        combined_gpas.max_gpa_value
    from combined_gpas
    join stg_academic_record
        on combined_gpas.k_student_academic_record = stg_academic_record.k_student_academic_record
        -- The same academic record key can appear in several ODS years.
        -- Match on api_year too, so the GPA joins only to the copy it came from.
        and combined_gpas.api_year = stg_academic_record.api_year
),

latest_ods as (
    select *
    from gpas_with_record
    qualify api_year = max(api_year) over (
        partition by
            tenant_code,
            lower(student_unique_id),
            school_year
    )
)

select * from latest_ods
