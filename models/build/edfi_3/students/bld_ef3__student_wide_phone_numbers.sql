with stg_student_phones as (
    select * from {{ ref('stg_ef3__stu_ed_org__telephones') }}
),
phones_wide as (
  select 
    k_student,
    tenant_code
    {%- if not is_empty_model('stg_ef3__stu_ed_org__telephones') -%},
    {{ dbt_utils.pivot(
      'phone_number_type',
      dbt_utils.get_column_values(ref('stg_ef3__stu_ed_org__telephones'), 'phone_number_type', order_by = 'phone_number_type'),
      agg='max',
      then_value='phone_number',
      else_value='null',
      suffix='_phone_number',
      quote_identifiers = False
    ) }}
    {%- endif %}
  from stg_student_phones
  group by k_student, tenant_code
)
select * from phones_wide 
