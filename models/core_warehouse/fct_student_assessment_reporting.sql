{# NOTE I'm leaving out a post hook with primary key for now.. it's not clear what the primary key is,
  it can technically differ based on what is configured in the sch_assoc and dedupe rules
#}

{# 
This model chains together a series of assessment reporting rules, to create a single stu-assessment model
that's ready for reporting (analysts and dashboards).

It uses macros so that this can be modular. Right now, this would mean other models use different ordering, 
or different combination of reporting macros, or override the rules done here. For example, you might write a 
model called 'fct_student_assessment_state_reporting' that uses different configured dedupe rules. Or, podium
might re-use the grade_levels macro to overwrite the decision made about grade levels here.
But should the order & combination of these be configurable here, to?
    
TODO think about the pros/cons of this method

Pros:
- macros make it easy to build a new reporting model
- configurability enables different rules by assessment, implementation, etc.

Cons:
- potential dangerous SQL injection
  -> security risk, e.g., someone configures SQL for Student Name to be returned as grade level, and that's exposed in a drop-down
  -> cost risk, accidental expensive queries that explode snowflake costs during dbt run
    --> e.g., someone creates an accidental 1:many performance level join that explodes records x10
- if one configures different rules by assessment within one model, this poses challenges:
  -> very long models with lots of CTEs?
  -> models that have a mix of rules, mix of grains?
  -> columns with mix of data types or definitions?
    
#}

with fct_student_assessment as (
    select * from {{ ref('fct_student_assessment') }}
),
fct_student_school_association as (
    select * from {{ ref('fct_student_school_association') }}
),
xwalk_seasons as (
  select * from {{ ref('xwalk_assessment_seasons') }}
),
xwalk_grade_levels as (
  select * from {{ ref('xwalk_assessment_grade_levels') }}
),
xwalk_pl_values as (
  select * from {{ ref('xwalk_assessment_performance_level_values') }}
),
xwalk_pl_thresholds as (
  select * from {{ ref('xwalk_assessment_performance_level_thresholds') }}
),
stu_assess_dedupe as (
  {# call macro to run dedupe, based on config in dbt project #}
  {{ 
    stu_assess_dedupe__by_assess(stu_assess_relation='fct_student_assessment',
                                 rules_by_assess={
                                  'nwea_map': {
                                    'assessment_identifier':'NWEA-Map',
                                    'namespace':'uri://www.nwea.org/map/Assessment',
                                    'partition_by':'k_student,k_assessment,school_year,v_other_results:"Test Name",v_other_results:"Term Name"',
                                    'order_by':'scale_score desc'
                                  }
                                 },
                                 default_rules={
                                  'partition_by': 'k_student,k_assessment',
                                  'order_by': 'scale_score desc'
                                 }) 
  }} 
),
stu_assess_sch as (
  {# call macro to association stu-assess to schools, based on config in dbt project #}
  {# {{stu_assess_school_assoc(stu_assess_relation='stu_assess_dedupe', #}
                            {# stu_sch_relation='fct_student_school_association',)}} #}
  {{
    stu_assess_school_assoc__by_assess(stu_assess_relation='fct_student_assessment',
                                       stu_sch_relation='fct_student_school_association',
                                       rules_by_assess={
                                         'nwea_map': {
                                           'assessment_identifier': 'NWEA-Map',
                                           'namespace': 'uri://www.nwea.org/map/Assessment',
                                           'join_rule': 'stu_assess.k_student = stu_sch.k_student
                                                         and stu_sch.is_latest_annual_entry',
                                           'rule_name': 'latest_entry',
                                           'dedupe_partition_by': 'k_student_assessment',
                                           'dedupe_order_by': 'entry_date,exit_withdraw_date desc'
                                         }
                                       })
  }}
                            
),
stu_assess_seasons as (
  {# call macro to add assessment season #}
  {{
    stu_assess_seasons(
      stu_assess_relation='stu_assess_sch',
      xwalk_seasons_relation='xwalk_seasons'
    )
  }}
),
stu_assess_grade_levels as (
  {# call macro to add assessment grade level, based on config in dbt project #}
  {{
    stu_assess_grade_levels(
      stu_assess_relation='stu_assess_seasons',
      xwalk_grade_levels_relation='xwalk_grade_levels'
    )
  }}
),
stu_assess_performance_levels as (
  {# call macro to add assessment performance level columns #}
  {{
    stu_assess_performance_levels(
      stu_assess_relation='stu_assess_grade_levels',
      xwalk_pl_values_relation='xwalk_pl_values',
      xwalk_pl_thresholds_relation='xwalk_pl_thresholds'
    ) 
  }}
)

select * from stu_assess_performance_levels