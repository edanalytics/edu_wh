-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{% set xwalk_academic_term_enabled = var('edu:xwalk_academic_terms:enabled', False) %}

with stg_academic_honors as (
    select * from {{ ref('stg_ef3__student_academic_records__academic_honors') }}
),
stu_academic_records as (
    select * from {{ ref('fct_student_academic_record') }}
),
joined_academic_honors as (
    select
      stg_academic_honors.tenant_code,
      -- we pull k_student, k_student_xyear from stu_academic record to ensure we are 
      -- including historical student records from years prior where the student is not in dim_student. 
      -- This logic is already implemented in fct_student_academic_record, we pull this in to avoid 
      -- duplicating the logic.
      stu_academic_records.k_student,
      stu_academic_records.k_student_xyear,
      stu_academic_records.k_lea, 
      stu_academic_records.k_school, 
      stu_academic_records.school_year,
      stu_academic_records.academic_term,
      stg_academic_honors.k_student_academic_record,
      stg_academic_honors.academic_honor_category_code,
      stg_academic_honors.honor_description,
      stg_academic_honors.honor_award_date,
      stg_academic_honors.honor_award_expires_date,
      stg_academic_honors.achievement_category_descriptor,
      stg_academic_honors.achievement_category_system,
      stg_academic_honors.achievement_title,
      stg_academic_honors.criteria,
      stg_academic_honors.criteria_url,
      stg_academic_honors.evidence_statement,
      stg_academic_honors.image_url,
      stg_academic_honors.issuer_name,
      stg_academic_honors.issuer_origin_url,
      {# add any extension columns configured from stg_ef3__student_academic_records__academic_honors #}
      {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_academic_records__academic_honors', flatten=False) }}
    from stg_academic_honors
    join stu_academic_records 
        on stg_academic_honors.k_student_academic_record = stu_academic_records.k_student_academic_record
),
{% if xwalk_academic_term_enabled %}
xwalk_academic_terms as (
    select * from {{ ref('xwalk_academic_terms') }}
),
{% endif %}
joined_with_xwalk as (
    select
        joined_academic_honors.*,
        {% if xwalk_academic_term_enabled %} 
        xwalk_academic_terms.sort_index
        {% else %}
        1 as sort_index
        {% endif %}
    from joined_academic_honors
    {% if xwalk_academic_term_enabled %}
    left join xwalk_academic_terms
        on joined_academic_honors.academic_term = xwalk_academic_terms.academic_term
    {% endif %}
),
dedupe_academic_honors as (
     {{
            dbt_utils.deduplicate(
                relation='joined_with_xwalk',
                partition_by='k_student_xyear, school_year, k_lea, k_school, academic_honor_category_code, honor_award_date',
                order_by = "sort_index nulls last" if xwalk_academic_term_enabled else "academic_term"
            )
        }}
)
select {{ edu_edfi_source.star('dedupe_academic_honors', except=['academic_term'])}}
from dedupe_academic_honors
order by tenant_code, k_student