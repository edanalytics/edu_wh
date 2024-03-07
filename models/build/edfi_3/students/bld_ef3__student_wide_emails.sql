with stg_student_emails as (
    select * from {{ ref('stg_ef3__stu_ed_org__emails') }}
),
email_types as (
    select * from {{ ref('xwalk_student_email_types') }}
),
emails_wide as (
  select 
    k_student,
    tenant_code
    {%- if not is_empty_model('xwalk_student_email_types') -%},
    {{ dbt_utils.pivot(
      'normalized_email_type',
      dbt_utils.get_column_values(ref('xwalk_student_email_types'), 'normalized_email_type'),
      agg='max',
      then_value='email_address',
      else_value='null',
      suffix='_email_address',
      quote_identifiers = False
    ) }}
    {%- endif %}
  from stg_student_emails
  join email_types 
    on stg_student_emails.email_type = email_types.original_email_type
  group by k_student, tenant_code
)
select * from emails_wide