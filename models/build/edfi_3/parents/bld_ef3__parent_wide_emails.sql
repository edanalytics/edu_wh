with stg_parent_emails as (
    -- parents were renamed to contacts in Data Standard v5.0
    -- the contacts staging tables contain both parent and contact records
    select * from {{ ref('stg_ef3__contacts__emails') }}
),
parent_email_types as (
    select * from {{ ref('xwalk_parent_email_types') }}
),
emails_wide as (
  select 
    k_contact as k_parent,
    tenant_code
    {%- if not is_empty_model('xwalk_parent_email_types') -%},
    -- note: this is already deduped to be the most recent record for a parent
    {{ dbt_utils.pivot(
      'normalized_email_type',
      dbt_utils.get_column_values(ref('xwalk_parent_email_types'), 'normalized_email_type'),
      agg='max',
      then_value='email_address',
      else_value='null',
      suffix='_email_address',
      quote_identifiers = False
    ) }}
    {%- endif %}
  from stg_parent_emails
  join parent_email_types 
    on stg_parent_emails.email_type = parent_email_types.original_email_type
  group by k_parent, tenant_code
)
select * from emails_wide