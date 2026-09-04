{% set subject_fields = var('edu:course:subject_fields', ['academic_subject']) %}
{% set transcript_grouping_fields = var('edu:course:transcript_grouping_fields', {}) %}
{% set has_honors = 'is_honors' in transcript_grouping_fields %}

with fct_course_transcripts as (
    select * from {{ ref('fct_course_transcripts') }}
),

dim_course as (
    select * from {{ ref('dim_course') }}
),

{% if has_honors %}
transcripts as (
    select
        fct_course_transcripts.*,
        {% for field, expr in transcript_grouping_fields.items() %}
        {{ expr }} as {{ field }}{% if not loop.last %},{% endif %}

        {% endfor %}
    from fct_course_transcripts
),
{% else %}
transcripts as (
    select * from fct_course_transcripts
),
{% endif %}

-- credits earned per student x year x subject, for years the student took that subject
annual_credits as (

    {% for field in subject_fields %}

    select
        transcripts.k_student_xyear,
        transcripts.tenant_code,
        transcripts.school_year,
        '{{ field }}' as subject_type,
        -- grouping() = 1 on rollup rows; distinguishes them from courses with a genuine NULL subject
        {# TODO: decide if 'subject' is correct naming, or if we should broaden to credits_agg_type? #}
        iff(
            grouping(dim_course.{{ field }}) = 0,
            coalesce(dim_course.{{ field }}, 'Unknown Subject'),
            'All Subjects'
        ) as course_subject,
        {% for tf in transcript_grouping_fields %}
        iff(grouping(transcripts.{{ tf }}) = 0, transcripts.{{ tf }}, null) as {{ tf }},
        {% endfor %}
        sum(transcripts.earned_credits) as credits_earned
    from transcripts
    join dim_course
        on transcripts.k_course = dim_course.k_course
    {# TODO: make this configurable #}
    where transcripts.course_attempt_result = 'P'
    {# Use grouping sets to aggregate by student+year+subject+transcript_grouping_fields, and student+year+subject+transcript_grouping_fields, and student+year #}
    group by grouping sets (
        {% if transcript_grouping_fields %}
        (
            transcripts.k_student_xyear,
            transcripts.tenant_code,
            transcripts.school_year,
            dim_course.{{ field }},
            {% for tf in transcript_grouping_fields %}
            transcripts.{{ tf }}{% if not loop.last %},{% endif %}

            {% endfor %}
        ),
        {% endif %}
        (
            transcripts.k_student_xyear,
            transcripts.tenant_code,
            transcripts.school_year,
            dim_course.{{ field }}
        ),
        (
            transcripts.k_student_xyear,
            transcripts.tenant_code,
            transcripts.school_year
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
    from transcripts

),

-- all subject x honor-level dimensions a student ever accumulated credits in,
-- with the first year they appear so the spine only starts from then
student_subject_dimensions as (

    select
        k_student_xyear,
        tenant_code,
        subject_type,
        course_subject,
        {% if has_honors %}
        is_honors,
        {% endif %}
        min(school_year) as first_school_year
    from annual_credits
    group by
        k_student_xyear,
        tenant_code,
        subject_type,
        course_subject
        {% if has_honors %}
        , is_honors
        {% endif %}

),

-- student x year x subject rows from first year in that subject onward,
-- so cumulative credits propagate forward without creating pre-history rows
spine as (

    select
        student_years.k_student_xyear,
        student_years.tenant_code,
        student_years.school_year,
        student_subject_dimensions.subject_type,
        student_subject_dimensions.course_subject
        {% if has_honors %}
        , student_subject_dimensions.is_honors
        {% endif %}
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
        {% if has_honors %}
        spine.is_honors,
        {% endif %}
        coalesce(annual_credits.credits_earned, 0) as credits_earned
    from spine
    left join annual_credits
        on  spine.k_student_xyear = annual_credits.k_student_xyear
        and spine.tenant_code     = annual_credits.tenant_code
        and spine.school_year     = annual_credits.school_year
        and spine.subject_type    = annual_credits.subject_type
        and spine.course_subject  = annual_credits.course_subject
        {% if has_honors %}
        and spine.is_honors is not distinct from annual_credits.is_honors
        {% endif %}

),

cumulative_credits as (

    select
        annual_with_spine.k_student_xyear,
        annual_with_spine.tenant_code,
        annual_with_spine.school_year,
        annual_with_spine.subject_type,
        annual_with_spine.course_subject,
        {% for tf in transcript_grouping_fields %}
        annual_with_spine.{{ tf }},
        {% endfor %}
        sum(annual_with_spine.credits_earned) over (
            partition by
                annual_with_spine.k_student_xyear,
                annual_with_spine.subject_type,
                annual_with_spine.course_subject
                {% for tf in transcript_grouping_fields %}
                , annual_with_spine.{{ tf }}
                {% endfor %}
            order by annual_with_spine.school_year
            rows between unbounded preceding and current row
        ) as cumulative_credits

    from annual_with_spine

)

select * from cumulative_credits
