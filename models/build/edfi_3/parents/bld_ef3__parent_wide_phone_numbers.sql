with stg_parent_phones as (
    select * from {{ ref('stg_ef3__parents__telephones') }}
),
parent_phone_number_types as (
    select * from {{ ref('xwalk_parent_phone_number_types') }}
),
phones_wide as (
  select 
    k_parent,
    tenant_code
    {%- if not is_empty_model('xwalk_parent_phone_number_types') -%},
    -- note: this is already deduped to be the most recent record for a parent
    {{ dbt_utils.pivot(
      'normalized_phone_number_type',
      dbt_utils.get_column_values(ref('xwalk_parent_phone_number_types'), 'normalized_phone_number_type'),
      agg='max',
      then_value='phone_number',
      else_value='null',
      suffix='_phone_number',
      quote_identifiers = False
    ) }}
    {%- endif %}
  from stg_parent_phones
  join parent_phone_number_types
    on stg_parent_phones.phone_number_type = parent_phone_number_types.original_phone_number_type
  group by k_parent, tenant_code
)
select * from phones_wide