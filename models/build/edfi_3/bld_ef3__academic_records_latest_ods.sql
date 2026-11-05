with stg_academic_record as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),

with stg_academic_record as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),

-- The same student's school year academic records can show up in several ODS years.
-- So here, we keep the records from the newest ODS that has the academic record for the student-school_year combination.
-- For example:
    -- Student | School Year | API Year | Ed Org   | Term       | Keep?
    -- --------|-------------|----------|----------|------------|------
    -- 123     | 2023        | 2023     | School A | Fall       | Yes
    -- 123     | 2024        | 2024     | District | Year Round | No (because a newer ODS has data for the same student-school_year)
    -- 123     | 2024        | 2025     | School A | Fall       | Yes
    -- 123     | 2024        | 2025     | School A | Spring     | Yes

-- For 2024, we take both records from the 2025 ODS and leave out the older copy.
-- For 2023, we keep the original record because it wasn't sent again in a newer ODS.

-- Notice that the ed org and term changed between the two copies of 2024.
-- We don't use those fields to choose an ODS, we take the newer set together,
-- rather than treating the changed fields as a reason to keep the older record too.

-- Diplomas come from the ODS chosen here.
-- Transcripts and GPAs make their own choice, since a newer ODS can have
-- academic records without the corresponding courses or GPA values.

latest_ods as (
    select *
    from stg_academic_record
    qualify api_year = max(api_year) over (
        partition by
            tenant_code,
            lower(student_unique_id),
            school_year
    )
)

select * from latest_ods

latest_ods as (
    select *
    from stg_academic_record
    qualify api_year = max(api_year) over (
        partition by
            tenant_code,
            lower(student_unique_id),
            school_year
    )
)

select * from latest_ods
