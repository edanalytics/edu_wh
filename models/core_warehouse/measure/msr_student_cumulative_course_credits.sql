{% set subject_fields = var('edu:course:subject_fields', ['academic_subject']) %}
{% set transcript_grouping_fields = var('edu:course:transcript_grouping_fields', {}) %}

with fct_course_transcripts as (
    select * from {{ ref('fct_course_transcripts') }}
),

dim_course as (
    select * from {{ ref('dim_course') }}
),

-- credits earned per student x year x subject, for years the student took that subject.
-- subject_fields each produce a union block grouped by that dim_course field.
-- transcript_grouping_fields each produce an additional union block filtered by their
-- expression and grouped by their configured subject_field, so transcript-level
-- dimensions (e.g. honors) appear as additional subject_type values rather than
-- cross-cut columns.
annual_credits as (

    {% for field in subject_fields %}

    select
        fct_course_transcripts.k_student_xyear,
        fct_course_transcripts.tenant_code,
        fct_course_transcripts.school_year,
        '{{ field }}' as subject_type,
        -- grouping() = 1 on rollup rows; distinguishes them from courses with a genuine NULL subject
        {# TODO: decide if 'subject' is correct naming, or if we should broaden to credits_agg_type? #}
        iff(
            grouping(dim_course.{{ field }}) = 0,
            coalesce(dim_course.{{ field }}, 'Unknown Subject'),
            'All Subjects'
        ) as course_subject,
        sum(fct_course_transcripts.earned_credits) as credits_earned
    from fct_course_transcripts
    join dim_course
        on fct_course_transcripts.k_course = dim_course.k_course
    {# TODO: make this configurable #}
    where fct_course_transcripts.course_attempt_result = 'P'
    group by grouping sets (
        (
            fct_course_transcripts.k_student_xyear,
            fct_course_transcripts.tenant_code,
            fct_course_transcripts.school_year,
            dim_course.{{ field }}
        ),
        (
            fct_course_transcripts.k_student_xyear,
            fct_course_transcripts.tenant_code,
            fct_course_transcripts.school_year
        )
    )

    {% if not loop.last or transcript_grouping_fields %}union all{% endif %}

    {% endfor %}

    {% for tgf_name, tgf in transcript_grouping_fields.items() %}

    select
        fct_course_transcripts.k_student_xyear,
        fct_course_transcripts.tenant_code,
        fct_course_transcripts.school_year,
        '{{ tgf_name }}' as subject_type,
        iff(
            grouping(dim_course.{{ tgf.subject_field }}) = 0,
            coalesce(dim_course.{{ tgf.subject_field }}, 'Unknown Subject'),
            'All Subjects'
        ) as course_subject,
        sum(fct_course_transcripts.earned_credits) as credits_earned
    from fct_course_transcripts
    join dim_course
        on fct_course_transcripts.k_course = dim_course.k_course
    where fct_course_transcripts.course_attempt_result = 'P'
      and {{ tgf.filter }}
    group by grouping sets (
        (
            fct_course_transcripts.k_student_xyear,
            fct_course_transcripts.tenant_code,
            fct_course_transcripts.school_year,
            dim_course.{{ tgf.subject_field }}
        ),
        (
            fct_course_transcripts.k_student_xyear,
            fct_course_transcripts.tenant_code,
            fct_course_transcripts.school_year
        )
    )

    {% if not loop.last %}union all{% endif %}

    {% endfor %}

),

-- all school years in which a student appears in any transcript
student_years as (

    select distinct
        k_student_xyear,
        tenant_code,
        school_year
    from fct_course_transcripts

),

-- all subject dimensions a student ever accumulated credits in,
-- with the first year they appear so the spine only starts from then
student_subject_dimensions as (

    select
        k_student_xyear,
        tenant_code,
        subject_type,
        course_subject,
        min(school_year) as first_school_year
    from annual_credits
    group by 1, 2, 3, 4

),

-- student x year x subject rows from first year in that subject onward,
-- so cumulative credits propagate forward into years with no new courses
spine as (

    select
        student_years.k_student_xyear,
        student_years.tenant_code,
        student_years.school_year,
        student_subject_dimensions.subject_type,
        student_subject_dimensions.course_subject
    from student_years
    inner join student_subject_dimensions
        on  student_years.k_student_xyear = student_subject_dimensions.k_student_xyear
        and student_years.tenant_code     = student_subject_dimensions.tenant_code
        and student_years.school_year    >= student_subject_dimensions.first_school_year

),

annual_with_spine as (

    select
        spine.k_student_xyear,
        spine.tenant_code,
        spine.school_year,
        spine.subject_type,
        spine.course_subject,
        coalesce(annual_credits.credits_earned, 0) as credits_earned
    from spine
    left join annual_credits
        on  spine.k_student_xyear = annual_credits.k_student_xyear
        and spine.tenant_code     = annual_credits.tenant_code
        and spine.school_year     = annual_credits.school_year
        and spine.subject_type    = annual_credits.subject_type
        and spine.course_subject  = annual_credits.course_subject

),

cumulative_credits as (

    select
        annual_with_spine.k_student_xyear,
        annual_with_spine.tenant_code,
        annual_with_spine.school_year,
        annual_with_spine.subject_type,
        annual_with_spine.course_subject,
        sum(annual_with_spine.credits_earned) over (
            partition by
                annual_with_spine.k_student_xyear,
                annual_with_spine.subject_type,
                annual_with_spine.course_subject
            order by annual_with_spine.school_year
            rows between unbounded preceding and current row
        ) as cumulative_credits

    from annual_with_spine

)

select * from cumulative_credits
