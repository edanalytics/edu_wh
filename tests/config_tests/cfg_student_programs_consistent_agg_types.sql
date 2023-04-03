/*
Find records where the is_active, is_annual, and is_ever values are 
not consistent across each indicator_name. This can occur when there 
are many program_name values mapped to a single indicator_name.
*/
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with xwalk_student_programs as (
    select * from {{ ref('xwalk_student_programs') }}
),
grouped as (
    select 
        indicator_name,
        is_active,
        is_annual,
        is_ever
    from xwalk_student_programs
    group by indicator_name, is_active, is_annual, is_ever
),
counts as (
    select 
        indicator_name, 
        count(*) as count_unique_configs
    from grouped
    group by indicator_name
),
joined as (
    select xwalk_student_programs.*
    from xwalk_student_programs
    join counts
        on xwalk_student_programs.indicator_name = counts.indicator_name
    where count_unique_configs > 1
)
select * from joined
