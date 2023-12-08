{# 
    This macro is used by assessment reporting models to create adjusted titles/labels for assessments, based
    on config set by the stadium implementation.

    This might be used in cases where dim_assessment.assessment_title is not specific enough to make 
    judgements on certain business rules. For example, for assessment_title 'My State Assessment', some
    assessments may be unique to a GRADE & SUBJECT (e.g. the Math 04 exam), while others are unique to 
    a COURSE (e.g. the Algebra I exam). 

    Example configuration:
    ```
    'bi:assessment_labels':
      'my_state assessment':
        'MY STATE ASSESS HS COURSE EXAMS':
            label: "concat_ws(' ', dim_assessment.assessment_title, v_other_results:Course)"
            when: v_other_results:Course != 'no_course'
        'MY STATE ASSESS PRE-K EXAMS':
            label: "concat_ws(' ', dim_assessment.assessment_title, 'Pre-K')"
            when: when_assessed_grade_level = 'PK'
    ```

    Example macro call:
    ```
        stu_assess_labels(label_rules=var('bi:assessment_labels')) as assessment_label
    ```

    Example resulting SQL
    ```when dim_assessment.assessment_identifier = 'my_state_assessment'
           and v_other_results:Course != 'no_course'
         then concat_ws(' ', dim_assessment.assessment_title, v_other_results:Course)
       when dim_assessment.assessment_identifier = 'my_state_assessment'
           and when_assessed_grade_level = 'PK'
         then concat_ws(' ', dim_assessment.assessment_title, 'Pre-K')
       else dim_assessment.assessment_title
       end as assessment_label
    ```
    Then, `assessment_label` can be used later on to join with xwalks that transform, convert
    scores to performance levels, add labels, etc. Or just used as a display label for dashboards.
    You can run this macro mutliple times with multiple versions of configs to create multiple
    versions of label columns, too.

#}
{%- macro stu_assess_labels(stu_assess_relation='fct_student_assessment',
                            label_rules={},
                            label_var='assessment_label',
                            default_label='dim_assessment.assessment_title') -%}

    {% if not label_rules -%}
        {{default_label}}
    {%- else -%}
    case
    {# loop over assessment ids & labels configured to have an adjusted title #}
    {% for assessment_identifier in label_rules -%}

        {%- for label in label_rules[assessment_identifier] -%}

            when dim_assessment.assessment_identifier = '{{ assessment_identifier }}'
                and {{ label_rules[assessment_identifier][label]['when'] }}
              then {{ label_rules[assessment_identifier][label][label_var] }} 

        {% endfor -%}
        
    {%- endfor -%}
        else {{default_label}}
    end
    {%- endif %}

{%- endmacro -%}

