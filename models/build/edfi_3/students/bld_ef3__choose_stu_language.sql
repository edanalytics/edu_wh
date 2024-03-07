with stu_language as (
    select * from {{ ref('stg_ef3__stu_ed_org__languages') }}
),
choose_language as (
    -- pick a single language use,
    -- defaults to home language
        {{
        row_pluck('stu_language',
                key='k_student',
                column='language_use', 
                preferred=var('edu:stu_language:language_use')
                )
    }}
)
select * from choose_language