{# edu:course:credit_dimensions defines the subject_type × course_subject breakdown in the output.
   Each key becomes a subject_type value. Two forms:
     bare key (null value)  → groups all passing transcripts by dim_course.<key>
     {field, filter}        → groups transcripts matching `filter` by dim_course.<field>, named by the key
   Extend with partner-specific dim_course fields or transcript-level cuts (e.g. honors, CTE). #}
{% set credit_dimensions = var('edu:course:credit_dimensions', {'academic_subject': none}) %}

with fct_course_transcripts as (
    select * from {{ ref('fct_course_transcripts') }}
),

dim_course as (
    select * from {{ ref('dim_course') }}
),

-- credits earned per student x year x subject, for years the student took that subject.
-- each credit_dimension produces a union block; filtered dimensions (those with `filter`)
-- appear as additional subject_type values rather than cross-cut columns.
annual_credits as (

    {% for dim_name, dim_config in credit_dimensions.items() %}
    {% set field = (dim_config or {}).get('field', dim_name) %}
    {% set filter_expr = (dim_config or {}).get('filter', none) %}

    select
        fct_course_transcripts.k_student_xyear,
        fct_course_transcripts.tenant_code,
        fct_course_transcripts.school_year,
        '{{ dim_name }}' as subject_type,
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
    where fct_course_transcripts.course_attempt_result in ('{{ var("edu:course_transcripts:passing_results", ["P"]) | join("', '") }}')
    {% if filter_expr %}  and {{ filter_expr }}{% endif %}
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

    {% if not loop.last %}union all{% endif %}

    {% endfor %}

),

-- student x year x subject rows from first year in that subject onward,
-- so cumulative credits propagate forward into years with no new courses
spine as (

    select sy.k_student_xyear, sy.tenant_code, sy.school_year, sd.subject_type, sd.course_subject
    from (
        select distinct k_student_xyear, tenant_code, school_year
        from fct_course_transcripts
    ) as sy
    join (
        select k_student_xyear, tenant_code, subject_type, course_subject, min(school_year) as first_school_year
        from annual_credits
        group by 1, 2, 3, 4
    ) as sd
        on  sy.k_student_xyear = sd.k_student_xyear
        and sy.tenant_code     = sd.tenant_code
        and sy.school_year    >= sd.first_school_year

),

final as (

    select
        spine.k_student_xyear,
        spine.tenant_code,
        spine.school_year,
        spine.subject_type,
        spine.course_subject,
        sum(coalesce(ac.credits_earned, 0)) over (
            partition by spine.k_student_xyear, spine.subject_type, spine.course_subject
            order by spine.school_year
            rows between unbounded preceding and current row
        ) as cumulative_credits
    from spine
    left join annual_credits as ac
        on  spine.k_student_xyear = ac.k_student_xyear
        and spine.tenant_code     = ac.tenant_code
        and spine.school_year     = ac.school_year
        and spine.subject_type    = ac.subject_type
        and spine.course_subject  = ac.course_subject

)

select * from final
