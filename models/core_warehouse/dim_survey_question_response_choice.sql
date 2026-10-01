{{
    config(
        post_hook=[
            "alter table {{ this }} alter column k_survey_question set not null",
            "alter table {{ this }} alter column sort_order set not null",
            "alter table {{ this }} add primary key (k_survey_question, sort_order)",
        ]
    )
}}

{%- set xwalk_response_values = var('edu:xwalk_survey_question_response_values:enabled', False) -%}

with stg_survey_question_response_choices as (
    select * from {{ ref('stg_ef3__survey_questions__response_choices') }}
),

{% if xwalk_response_values -%}

xwalk_response_values as (
    select
        survey_id,
        namespace,
        question_code,
        text_value,
        normalized_sort_order,
        normalized_numeric_value,
        normalized_text_value

    from {{ ref('xwalk_survey_question_response_values') }}
),

formatted as (
    select
        stg_survey_question_response_choices.k_survey_question,
        stg_survey_question_response_choices.tenant_code,
        stg_survey_question_response_choices.sort_order,
        stg_survey_question_response_choices.numeric_value,
        stg_survey_question_response_choices.text_value,
        xwalk_response_values.normalized_sort_order,
        xwalk_response_values.normalized_numeric_value,
        xwalk_response_values.normalized_text_value

    from stg_survey_question_response_choices

    left join xwalk_response_values
        on stg_survey_question_response_choices.survey_id = xwalk_response_values.survey_id
        and stg_survey_question_response_choices.namespace = xwalk_response_values.namespace
        and stg_survey_question_response_choices.question_code = xwalk_response_values.question_code
        
        -- Joining on text_value because it's more natural to configure a xwalk with,
        -- but technically it's not part of the natural key (sort_order is)
        and stg_survey_question_response_choices.text_value = xwalk_response_values.text_value
)

{% else %}

formatted as (
    select
        stg_survey_question_response_choices.k_survey_question,
        stg_survey_question_response_choices.tenant_code,
        stg_survey_question_response_choices.sort_order,
        stg_survey_question_response_choices.numeric_value,
        stg_survey_question_response_choices.text_value

    from stg_survey_question_response_choices
)

{% endif %}

select * from formatted
