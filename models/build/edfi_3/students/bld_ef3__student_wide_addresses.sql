with stg_student_address as (
    select * from {{ ref('stg_ef3__stu_ed_org__addresses') }}
),
address_wide as (
  select 
    k_student,
    tenant_code
    {%- if not is_empty_model('stg_ef3__stu_ed_org__addresses') -%},
    {{ dbt_utils.pivot(
      'address_type',
      dbt_utils.get_column_values(ref('stg_ef3__stu_ed_org__addresses'), 'address_type', order_by = 'address_type'),
      agg='max',
      then_value='street_address',
      else_value='null',
      suffix='_address',
      quote_identifiers = False
    ) }}
    {%- endif %}
  from stg_student_address
  group by k_student, tenant_code
)
select * from address_wide