{# 
This macro takes configuration inputs and generates school associations for student-assessment records. 
These associations can then be integrated into assessment reporting models. 

For specific configurations based on different assessment rules, see "stu_assess_school_assoc__by_assess" macro below.
#}
{%- macro stu_assess_school_assoc(stu_assess_relation,
                                  stu_sch_relation,
                                  rule_name='longest_enr_by_school',
                                  join_rule='stu_assess.k_student = stu_sch.k_student',
                                  filter='where true',
                                  dedupe_partition_by='k_student_assessment,k_school',
                                  dedupe_order_by='entry_date,exit_withdraw_date desc') -%}
               
  select 
    stu_assess.*,
    stu_sch.k_school,
    stu_sch.entry_grade_level as grade_level__sch_assoc,
    '{{ rule_name }}' as sch_assoc_rule
  from {{ stu_assess_relation }} stu_assess
  join {{ ref('dim_assessment') }} dim_assessment
    on stu_assess.k_assessment = dim_assessment.k_assessment
  join {{ stu_sch_relation }} stu_sch
    on {{ join_rule }}
  {{ filter }}
  qualify 
    row_number() over (
      partition by {{ dedupe_partition_by }}
      order by {{ dedupe_order_by }}
    ) = 1

{%- endmacro -%}

{# MACRO FOR ASSESSMENT-SPECIFIC RULES

Certain assessments may need different rules for school association.
For example, for a summative state assessment, the district wants to associate records only if test date is within
the enrollment window. But for an interim assessment, the district wants to associate all records to ALL CURRENT enrollments.
To achieve this, configure the rules for each assessment in a dbt variable, then pass the variable to `sch_assoc_rules`.

#}
{%- macro stu_assess_school_assoc__by_assess(stu_assess_relation,
                                             stu_sch_relation,
                                             rules_by_assess={},
                                             default_rules={'rule_name': 'longest_enr_by_school',
                                                            'join_rule': 'stu_assess.k_student = stu_sch.k_student',
                                                            'filter': 'where true',
                                                            'dedupe_partition_by': 'k_student_assessment,k_school',
                                                            'dedupe_order_by': 'entry_date,exit_withdraw_date desc'}) -%}

{# LOOP over config, create a sch-assoc CTE for each assessment #}
{# default to empty string '', so if none configured, sql below compiles to `not in ('')`, which is valid and should return all rows 
        is there a cleaner way to handle this? #}
{%- set rules_applied = ["''"] -%} 
{%- for key, rules_dict in rules_by_assess.items() %}
  {%- if loop.first -%} with {% endif %}
  stu_assess_sch_{{key}} as (
    {{ stu_assess_school_assoc(stu_assess_relation=stu_assess_relation,
                               stu_sch_relation=stu_sch_relation,
                               rule_name=rules_dict['rule_name'],
                               join_rule=rules_dict['join_rule'],
                               filter="where dim_assessment.assessment_identifier = '{}'
                                        and dim_assessment.namespace = '{}'".format(rules_dict["assessment_identifier"], rules_dict["namespace"]),
                               dedupe_partition_by=rules_dict['dedupe_partition_by'],
                               dedupe_order_by=rules_dict['dedupe_order_by']
    ) }}
  ){%- if not loop.last -%},{%- endif -%}
  {%- set _ = rules_applied.append("'{}__{}'".format(rules_dict['assessment_identifier'], rules_dict['namespace'])) -%}
{%- endfor %}
{# STACK each sch-assoc CTE from loop #}
{%- for key in rules_by_assess %}
  select *
  from stu_assess_sch_{{key}}
  union all
{%- endfor %}
  {# STACK with default rule for assessments that don't have a stu-school-assoc rule configured #}
  {{ stu_assess_school_assoc(stu_assess_relation=stu_assess_relation,
                             stu_sch_relation=stu_sch_relation,
                             rule_name=default_rules['rule_name'],
                             join_rule=default_rules['join_rule'],
                             filter="where dim_assessment.assessment_identifier||'__'||dim_assessment.namespace not in ({})".format(rules_applied|join(',')),
                             dedupe_partition_by=default_rules['dedupe_partition_by'],
                             dedupe_order_by=default_rules['dedupe_order_by']
    ) }}

{%- endmacro -%}