{# MACRO FOR ASSESSMENT-SPECIFIC RULES

This macro takes assessment-specific configuration inputs and generates deduplication code for student-assessment records.

Certain assessments may need different rules for deduplication. 
For example, the partner may want to dedupe a state assessment by stu/assess/school/year, while deduping NWEA MAP by 
stu/assess/school/year/date.

TODO: should we have a non-by_assess version? Leaving out for now, bc it would basically just be recreating dbt_utils.deduplicate.
But we could make it a wrapper that adds dedupe_rule as a column, similar to sch_assoc_rule in stu_assess_school_assoc().
And, we could consider having pre-built rules for people to quickly implement, e.g. 'max_scale_score_by_stu_assess' -- 
not sure how much value that would add, though.

TODO: are we sure that rules will always be at level of assess_id & namespace? what if someone wants different rules by year or by tenant?
 Could we make more flexible by adding arguments to the macro?

#}
{% macro stu_assess_dedupe__by_assess(stu_assess_relation,
                                      rules_by_assess={},
                                      default_rules={}) %}


  {# LOOP over config, create dedupe CTE for each assessment #}
  {# default to empty string '', so if none configured, sql below compiles to `not in ('')`, which is valid and should return all rows 
          is there a cleaner way to handle this? #}
  {%- set rules_applied = ["''"] -%} 
  with
  {%- for key, rules_dict in rules_by_assess.items() %}
    stu_assess_dedupe_{{key}} as (
      select 
        stu_assess.*
      from {{ stu_assess_relation }} stu_assess
      join {{ ref('dim_assessment') }} dim_assessment
        on stu_assess.k_assessment = dim_assessment.k_assessment
      where dim_assessment.assessment_identifier = '{{ rules_dict["assessment_identifier"] }}'
        and dim_assessment.namespace = '{{ rules_dict["namespace"] }}'
    ),
    deduped_{{key}} as (
      {%- set this_relation = 'stu_assess_dedupe_'~key -%}
      {{
          dbt_utils.deduplicate(
              relation=this_relation,
              partition_by=rules_dict["partition_by"],
              order_by=rules_dict["order_by"]
          )
      }}
    ),
    {%- set _ = rules_applied.append("'{}__{}'".format(rules_dict['assessment_identifier'], rules_dict['namespace'])) -%}

  {%- endfor %}
    stu_assess_dedupe_default as (
      select 
        stu_assess.*
      from {{ stu_assess_relation }} stu_assess
      join {{ ref('dim_assessment') }} dim_assessment
        on stu_assess.k_assessment = dim_assessment.k_assessment
      where dim_assessment.assessment_identifier||'__'||dim_assessment.namespace not in ({{rules_applied|join(',')}})
    )
  {# STACK each dedupe CTE from loop #}
  {% for key in rules_by_assess %}
    select *
    from deduped_{{key}}
    union all
  {% endfor -%}
    {# STACK with default: assessments that don't have a dedupe rule configured #}
    {%- if default_rules %}
      {{
          dbt_utils.deduplicate(
              relation='stu_assess_dedupe_default',
              partition_by=default_rules["partition_by"],
              order_by=default_rules["order_by"]
          )
      }}
    {%- else %}
    select * from stu_assess_dedupe_default
    {%- endif -%}

{% endmacro %}