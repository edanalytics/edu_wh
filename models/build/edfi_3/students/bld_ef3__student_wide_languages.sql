with stg_student_languages as (
    select * from {{ ref('stg_ef3__stu_ed_org__languages') }}
),
language_wide as (
  select 
    k_student,
    tenant_code
    {%- if not is_empty_model('stg_ef3__stu_ed_org__languages') -%},
    {{ dbt_utils.pivot(
      'language_use',
      dbt_utils.get_column_values(ref('stg_ef3__stu_ed_org__languages'), 'language_use', order_by = 'language_use'),
      agg='max',
      then_value='code_value',
      else_value='null',
      quote_identifiers = False
    ) }}
    {%- endif %}
  from stg_student_languages
  group by k_student, tenant_code
)
select * from language_wide