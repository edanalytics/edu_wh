{# 
This macro is used by fct_student_assessment_reporting and other assessment models,
to add columns for the asssesment record's associated "season"

TODO: determine if this is really broadly useful, or if there's ways to make it more broadly useful
I think that we should be careful to distinguish somewhat arbitrary "season" definitions from
administration windows, which would require separate metadata by assessment/year
#}
{% macro stu_assess_seasons(stu_assess_relation='fct_student_assessment',
                                   xwalk_assessment_seasons='xwalk_assessment_seasons') %}
  xwalk_assessment_seasons as (
    select * from {{ ref(xwalk_assessment_seasons) }}
  ),
  stu_assess_seasons as (
    select 
      stu_assess.*,
      sea.season_num,
      sea.season_name,
      sea.start_date as season_start_date,
      sea.end_date as season_end_date
    from {{ stu_assess_relation }} stu_assess
    left join {{ xwalk_assessment_seasons }} sea
      on stu_assess.school_year = sea.school_year
      and to_date(stu_assess.administration_date) >= sea.start_date 
      and to_date(stu_assess.administration_date) <= sea.end_date
  )
{% endmacro %}