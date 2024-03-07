with stg_student_phones as (
    select * from {{ ref('stg_ef3__stu_ed_org__telephones') }}
),
phone_number_types as (
    select * from {{ ref('xwalk_student_phone_number_types') }}
),
phones_wide as (
  select 
    k_student,
    tenant_code
    {%- if not is_empty_model('xwalk_student_phone_number_types') -%},
    {{ dbt_utils.pivot(
      'normalized_phone_number_type',
      dbt_utils.get_column_values(ref('xwalk_student_phone_number_types'), 'normalized_phone_number_type'),
      agg='max',
      then_value='phone_number',
      else_value='null',
      suffix='_phone_number',
      quote_identifiers = False
    ) }}
    {%- endif %}
  from stg_student_phones
  join phone_number_types
    on stg_student_phones.phone_number_type = phone_number_types.original_phone_number_type
  group by k_student, tenant_code
)
select * from phones_wide