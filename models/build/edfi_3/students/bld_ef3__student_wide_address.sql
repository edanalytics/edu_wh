with stg_student_address as (
    select * from {{ ref('stg_ef3__stu_ed_org__addresses') }}
),
address_types as (
    select * from {{ ref('xwalk_student_address_types') }}
),
address_wide as (
  select 
    k_student,
    tenant_code
    {%- if not is_empty_model('xwalk_student_address_types') -%},
    {{ dbt_utils.pivot(
      'normalized_address_type',
      dbt_utils.get_column_values(ref('xwalk_student_address_types'), 'normalized_address_type'),
      agg='max',
      then_value='street_address',
      else_value='null',
      suffix='_address',
      quote_identifiers = False
    ) }}
    {%- endif %}
  from stg_student_address
  join address_types 
    on stg_student_address.address_type = address_types.original_address_type
  group by k_student, tenant_code
)
select * from address_wide