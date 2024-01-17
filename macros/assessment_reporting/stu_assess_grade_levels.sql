{# 
This macro takes configuration inputs and generates code to map stu-assess grade levels to a normalized format,
 and choose the source of grade level (e.g. assessed vs. enrolled grade)

#}
{% macro stu_assess_grade_levels(stu_assess_relation,
                                 xwalk_grade_levels_relation,
                                 grade_level_rules) -%}
    
    select 
      stu_assess.*,
      {% if grade_level_rules -%}
      {{grade_level_rules}}
      {% else -%}
      {# default rule: prefer normalized from xwalk, then assessed, then dim stu grade #}
      coalesce(xwalk_grades.normalized_grade_level::varchar, 
               stu_assess.when_assessed_grade_level::varchar,
               dim_student.grade_level::varchar) as reporting_grade_level
      {% endif -%}
    from {{ stu_assess_relation }} stu_assess
    join {{ ref('dim_assessment') }} dim_assessment
      on stu_assess.k_assessment = dim_assessment.k_assessment
    join {{ ref('dim_student') }} dim_student
      on stu_assess.k_student = dim_student.k_student
    left join {{ xwalk_grade_levels_relation }} xwalk_grades
      on dim_assessment.assessment_identifier = xwalk_grades.assessment_identifier
      and dim_assessment.namespace = xwalk_grades.namespace
      and stu_assess.when_assessed_grade_level = xwalk_grades.edfi_grade_level

{%- endmacro %}