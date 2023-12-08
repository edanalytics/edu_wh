{# 
This macro is used by fct_student_assessment_reporting and other assessment models,
to run configured association of stu-assessment records to schools.

Certain implementations or certain assessments may need different rules for school association.
For example, partner 1 wants to associate records only if test date is within the enrollment window,
while partner 2 wants to associate all assessments to CURRENT enrollments.

TODO: determine how to handle a mixed approach, e.g. two applications with different rules?

TODO: should certain dedupes happen here? e.g. if joining to stu-school creates duplicates, should those 
be resolved here, or resolved in the `student_assessment_dedupe` macro?

#}
{% macro stu_assess_school_assoc(stu_assess_relation='fct_student_assessment',
                                 sch_assoc_rules=var('edu:assessment_reporting:sch_assoc_rules', {}),
                                 dedupe_partition_by='k_student_assessment,k_school',
                                 dedupe_order_by='entry_date,exit_withdraw_date desc') %}
{# default to empty string '', so if none configured, sql below compiles to `not in ('')`, which is valid and should return all rows 
        is there a cleaner way to handle this? #}
{%- set rules_applied = ["''"] -%}                        
  {%- for key, rules_dict in sch_assoc_rules.items() -%}
  stu_assess_sch_{{key}} as (
      select 
        fct_student_assessment.*,
        stu_sch.k_school,
        stu_sch.entry_grade_level as grade_level__sch_assoc,
        '{{ rules_dict["rule_name"] }}' as sch_assoc_rule
      from {{ stu_assess_relation }} fct_student_assessment
      join {{ ref('dim_assessment') }} dim_assessment
        on fct_student_assessment.k_assessment = dim_assessment.k_assessment
      join {{ ref('fct_student_school_association') }} stu_sch
        on {{ rules_dict['join_rule'] }}
      where dim_assessment.assessment_identifier = '{{ rules_dict["assessment_identifier"] }}'
        and dim_assessment.namespace = '{{ rules_dict["namespace"] }}'
  ),
  {%- set rules_applied = rules_applied.append("'"~rules_dict['assessment_identifier']~'__'~rules_dict["namespace"]~"'") -%}
  {%- endfor %}
  stu_assess_sch as (
    {%- for key in sch_assoc_rules -%}
    select *
    from stu_assess_sch_{{key}}
    union all
    {%- endfor -%}
    {# stack with default rule for assessments that don't have a stu-school-assoc rule configured #}
    {# TODO make the default rule configurable #}
    {# TODO/REVIEW THIS should the default be to NOT join at all? so as not to create bad duplicates by default? #}
    select 
      fct_student_assessment.*,
      stu_sch.k_school,
      stu_sch.entry_grade_level as grade_level__sch_assoc,
      'tested_during_enroll' as sch_assoc_rule
    from {{ stu_assess_relation }} fct_student_assessment
    join {{ ref('dim_assessment') }} dim_assessment
      on fct_student_assessment.k_assessment = dim_assessment.k_assessment
    {# default rule: assessment is within enrollment window #}
    join {{ ref('fct_student_school_association') }} stu_sch
      on fct_student_assessment.k_student = stu_sch.k_student
        and fct_student_assessment.administration_date >= stu_sch.entry_date 
        and (fct_student_assessment.administration_date <= stu_sch.exit_withdraw_date
              or stu_sch.exit_withdraw_date is NULL
            )
    {# only apply default to rows that haven't already had a rule applied #}
    where dim_assessment.assessment_identifier||'__'||dim_assessment.namespace not in ({{rules_applied|join(',')}})
    qualify 
      row_number() over (
        partition by {{ dedupe_partition_by }}
        order by {{ dedupe_order_by }}
      ) = 1
  )
{% endmacro %}