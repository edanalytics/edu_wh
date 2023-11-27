{# 
This macro is used by fct_student_assessment_reporting and other assessment models,
to run configured deduplication of student-assessment records based on configuration in the dbt project.

For example, the partner may want to dedupe a state assessment by stu/assess/school/year, while deduping NWEA map by stu/assess/school/year/date

This macro returns SQL code with CTEs that can slot into any dbt model where `fct_student_assessment` and `dim_assessment`
have been included as prior CTEs.

TODO: review this method of macros that create partial code. Is that confusing, is it bad programming practice?

TODO: are we sure that rules will always be at level of assess_id & namespace? what if someone wants different rules by year or by tenant?
    Could we make more flexible by adding arguments to the macro?
#}
{% macro stu_assess_dedupe(stu_assess_relation='fct_student_assessment',
                                          dedupe_rules=var('edu:assessment_reporting:dedupe_rules', {})) %}

{# default to empty string '', so if none configured, sql below compiles to `not in ('')`, which is valid and should return all rows 
        is there a cleaner way to handle this? #}
{%- set rules_applied = ["''"] -%}                        
{%- for key, rules_dict in dedupe_rules.items() -%}
  fct_student_assessment_{{key}} as (
      select 
        fct_student_assessment.*
      from {{ stu_assess_relation }} fct_student_assessment
      join {{ ref('dim_assessment') }} dim_assessment
        on fct_student_assessment.k_assessment = dim_assessment.k_assessment
      where dim_assessment.assessment_identifier = '{{ rules_dict["assessment_identifier"] }}'
        and dim_assessment.namespace = '{{ rules_dict["namespace"] }}'
  ),
  deduped_{{key}} as (
      {%- set this_relation = 'fct_student_assessment_'~key -%}
      {{
          dbt_utils.deduplicate(
              relation=this_relation,
              partition_by=rules_dict["partition_by"],
              order_by=rules_dict["order_by"]
          )
      }}
  ),
  {%- set rules_applied = rules_applied.append("'"~rules_dict['assessment_identifier']~'__'~rules_dict["namespace"]~"'") -%}
{%- endfor %}
  stu_assess_dedupe as (
    {% for key in dedupe_rules %}
    select *
    from deduped_{{key}}
    union all
    {% endfor -%}
  {# stack with assessments that don't have a dedupe rule configured #}
  {# todo: add a section for default (x-assessment) dedupe rule? should that be allowed if there's assess-specific rules configured? #}
    select 
      fct_student_assessment.*
    from {{ stu_assess_relation }} fct_student_assessment
    join {{ ref('dim_assessment') }} dim_assessment
      on fct_student_assessment.k_assessment = dim_assessment.k_assessment
    where dim_assessment.assessment_identifier||'__'||dim_assessment.namespace not in ({{rules_applied|join(',')}})
  )
{% endmacro %}