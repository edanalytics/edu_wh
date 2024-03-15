with stg_student_emails as (
    select * from {{ ref('stg_ef3__stu_ed_org__emails') }}
),
email_wide as (
  select 
    k_student,
    tenant_code
    {%- if not is_empty_model('stg_ef3__stu_ed_org__emails') -%},
    {{ dbt_utils.pivot(
      'email_type',
      dbt_utils.get_column_values(ref('stg_ef3__stu_ed_org__emails'), 'email_type', order_by = 'email_type'),
      agg='max',
      then_value='email_address',
      else_value='null',
      suffix='_email_address',
      quote_identifiers = False
    ) }}
    {%- endif %}
  from stg_student_emails
  group by k_student, tenant_code
)
select * from email_wide