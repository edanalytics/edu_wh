with stg_parent_emails as (
    select * from {{ ref('stg_ef3__parents__emails') }}
),
parent_email_types as (
    select * from {{ ref('xwalk_parent_email_types') }}
),
emails_wide as (
  select 
    k_parent,
    -- todo: do I need api year in here?
    {{ dbt_utils.pivot(
      'normalized_email_type',
      dbt_utils.get_column_values(ref('xwalk_parent_email_types'), 'normalized_email_type'),
      agg='listagg',
      then_value='email_address',
      suffix='_email_address'
    ) }}
  from stg_parent_emails
  join parent_email_types 
    on stg_parent_emails.email_type = parent_email_types.original_email_type
  group by k_parent
)
select * from emails_wide