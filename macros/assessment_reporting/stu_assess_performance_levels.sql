{# 
This macro is used by fct_student_assessment_reporting and other assessment models,
to add columns for the stu-asssesment record's performance levels, remapped to certain data types or scales

PARAM xwalk_assessment_performance_level_values: xwalk with convert raw performance level values to new values, e.g. display value, integer value, etc.
PARAM xwalk_assessment_performance_level_thresholds: xwalk to convert a score to a performance level using thresholds on a scale. useful if no performance level in the raw dataset

TODO: figure out cleaner and/or more configurable way to handle the joins to xwalks
#}
{% macro stu_assess_performance_levels(stu_assess_relation='fct_student_assessment',
                                       pl_rules=var('edu:assessment_reporting:pl_rules', {}),  
                                       xwalk_assessment_performance_level_values='xwalk_assessment_performance_level_values',
                                       xwalk_assessment_performance_level_thresholds= 'xwalk_assessment_performance_level_thresholds',
                                       assess_title_var=var('edu:assessment_reporting:adj_assess_title_var', 'dim_assessment.assessment_title')
                                       ) %}

  xwalk_assessment_performance_level_thresholds as (
    select * from {{ ref(xwalk_assessment_performance_level_thresholds) }}
  ),
  xwalk_assessment_performance_level_values as (
    select * from {{ ref(xwalk_assessment_performance_level_values) }}
  ),
  stu_assess_adj_titles as (
    select
      stu_assess.*,
      {{ stu_assess_labels(label_rules=var('edu:assessment_reporting:adj_assess_title_rules', {}),
                          label_var=assess_title_var
                          ) 
      }} as {{ assess_title_var }}
    from {{ stu_assess_relation }} stu_assess
    join {{ ref('dim_assessment') }} dim_assessment
      on stu_assess.k_assessment = dim_assessment.k_assessment
  ),
  stu_assess_pls as (
    select 
      stu_assess.*,
      pl_thresh.performance_level as performance_level__pl_thresh,
      pl_values.performance_level_int as performance_level__int,
      pl_values.performance_level_display_name as performance_level__display_name,
      pl_values.is_proficient as performance_level__is_proficient
    from stu_assess_adj_titles stu_assess
    join {{ ref('dim_assessment') }} dim_assessment
      on stu_assess.k_assessment = dim_assessment.k_assessment
    {# TODO should these joins be configurable? cleaner? both?
        for configurable, could do one join per configured assessment e.g. left join {{ thresholds }} pl_thresh__nwea_map on {{config'd join}} 
    #}
    left join xwalk_assessment_performance_level_thresholds pl_thresh
      on dim_assessment.assessment_identifier = pl_thresh.assessment_identifier
      and dim_assessment.namespace = pl_thresh.namespace
      and dim_assessment.school_year = pl_thresh.school_year
      -- equal_null is used because some assess don't have e.g. academic_subject and the xwalk should leave academic_subject blank in that case
      and equal_null(dim_assessment.academic_subject, pl_thresh.academic_subject)
      and equal_null(stu_assess.when_assessed_grade_level, pl_thresh.assessed_grade_level)
      {# TODO better way to have custom joins by "title"? add configurable join conditions? #}
       and equal_null({{assess_title_var}}, pl_thresh.assessment_title) 
      {# TODO config the score column #}
      and stu_assess.scale_score >= pl_thresh.lower_bound
      and stu_assess.scale_score <= pl_thresh.upper_bound
    left join xwalk_assessment_performance_level_values pl_values
      on dim_assessment.assessment_identifier = pl_values.assessment_identifier
      and dim_assessment.namespace = pl_values.namespace
      and coalesce(pl_thresh.performance_level, stu_assess.performance_level) = pl_values.performance_level
  )
{% endmacro %}