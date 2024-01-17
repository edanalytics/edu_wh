{# 
This macro returns SQL code with CTEs that can slot into any dbt model. For example, you might use in a reporting model for an application,
or in an analyst-facing model. 

This macro creates configured association of stu-assessment records to schools.

Certain assessments may need different rules for school association.
For example, for a summative state assessment, the district wants to associate records only if test date is within
the enrollment window. But for an interim assessment, the district wants to associate all records to ALL CURRENT enrollments.
To achieve this, configure the rules for each assessment in a dbt variable, then pass the variable to `sch_assoc_rules`.

#}
{%- macro stu_assess_school_assoc(stu_assess_relation,
                                  rules_by_assessment,
                                  rule_name='longest_enr_by_school',
                                  join_rule='stu_assess.k_student = stu_sch.k_student',
                                  dedupe_partition_by='k_student_assessment,k_school',
                                  dedupe_order_by='entry_date,exit_withdraw_date desc') -%}

{# IF given a dictionary of assess-specific rules, use the macro that creates assess-specific CTEs #}
{% if rules_by_assessment %}
  {{ stu_assess_school_assoc__by_assess(stu_assess_relation=stu_assess_relation,
                                        sch_assoc_rules=rules_by_assessment, 
                                        default_sch_assoc_rules={
                                          'rule_name': rule_name,
                                          'join_rule': join_rule,
                                          'dedupe_partition_by': dedupe_partition_by,
                                          'dedupe_order_by': dedupe_order_by,
                                        }) }}
{# ELSE just use one rule for all records #}
{% else %}               
  select 
    stu_assess.*,
    stu_sch.k_school,
    stu_sch.entry_grade_level as grade_level__sch_assoc,
    '{{ rule_name }}' as sch_assoc_rule
  from {{ stu_assess_relation }} stu_assess
  join {{ ref('dim_assessment') }} dim_assessment
    on stu_assess.k_assessment = dim_assessment.k_assessment
  {# default rule: assessment is within enrollment window #}
  join {{ ref('fct_student_school_association') }} stu_sch
    on {{ join_rule }}
  qualify 
    row_number() over (
      partition by {{ dedupe_partition_by }}
      order by {{ dedupe_order_by }}
    ) = 1
{% endif %}

{%- endmacro -%}

{# MACRO FOR ASSESSMENT-SPECIFIC RULES #}
{%- macro stu_assess_school_assoc__by_assess(stu_assess_relation,
              sch_assoc_rules={},
              default_sch_assoc_rules={}) -%}

{# LOOP over config, create a sch-assoc CTE for each assessment #}
{# default to empty string '', so if none configured, sql below compiles to `not in ('')`, which is valid and should return all rows 
        is there a cleaner way to handle this? #}
{%- set rules_applied = ["''"] -%} 
{%- for key, rules_dict in sch_assoc_rules.items() %}
  {%- if loop.first -%} with {% endif %}
  stu_assess_sch_filter_{{key}} as (
    select stu_assess.*
    from {{ stu_assess_relation }} stu_assess
    join {{ ref('dim_assessment') }}
      on stu_assess.k_assessment = dim_assessment.k_assessment
    where dim_assessment.assessment_identifier = '{{ rules_dict["assessment_identifier"] }}'
      and dim_assessment.namespace = '{{ rules_dict["namespace"] }}'
  ),
  stu_assess_sch_{{key}} as (
    {{ stu_assess_school_assoc(stu_assess_relation='stu_assess_sch_filter_'~key,
                               rule_name=rules_dict['rule_name'],
                               join_rule=rules_dict['join_rule'],
                               dedupe_partition_by=rules_dict['dedupe_partition_by'],
                               dedupe_order_by=rules_dict['dedupe_order_by']
    ) }}
  ),
  {%- set _ = rules_applied.append("'"~rules_dict['assessment_identifier']~'__'~rules_dict["namespace"]~"'") -%}
{%- endfor %}
stu_assess_sch_filter_default as (
  select stu_assess.*
  from {{ stu_assess_relation }} stu_assess
  join {{ ref('dim_assessment') }}
      on stu_assess.k_assessment = dim_assessment.k_assessment
  {# only apply default to rows that haven't already had a rule applied #}
  where dim_assessment.assessment_identifier||'__'||dim_assessment.namespace not in ({{rules_applied|join(',')}})
)
{# STACK each sch-assoc CTE from loop #}
{%- for key in sch_assoc_rules %}
  select *
  from stu_assess_sch_{{key}}
  union all
{%- endfor %}
  {# STACK with default rule for assessments that don't have a stu-school-assoc rule configured #}
  {{ stu_assess_school_assoc(stu_assess_relation='stu_assess_sch_filter_default',
                               rule_name=default_sch_assoc_rules['rule_name'],
                               join_rule=default_sch_assoc_rules['join_rule'],
                               dedupe_partition_by=default_sch_assoc_rules['dedupe_partition_by'],
                               dedupe_order_by=default_sch_assoc_rules['dedupe_order_by']
    ) }}

{%- endmacro -%}