with stg_network_associations as (
  select * from {{ ref('stg_ef3__education_organization_network_associations') }}
),

xwalk_network_school_assoc_types as (
  select * from {{ ref('xwalk_network_association_types') }}
  where association_type = 'school'
),

-- todo: is this a weird dependency?
dim_network as (
  select * from {{ ref('dim_network') }}
),

joined_stg_dim as (
  select
    stg.*,
    dim_network.network_purpose
  from stg_network_associations stg
  join dim_network
    on stg.k_network = dim_network.k_network

),

-- dedupe so that for each school x network_purpose, the latest end date, latest begin date is selected
-- the unique key in edfi is on ed org & network, meaning ed orgs can be mapped to multiple networks, even within one "purpose"
-- note, nulls are sorted to top when using desc
deduped as (
  {{
        dbt_utils.deduplicate(
            relation='joined_stg_dim',
            partition_by='k_school, network_purpose',
            order_by='end_date desc, begin_date desc'
        )
    }}

),

wide as (

  select
    deduped.k_school,
    deduped.tenant_code,
    deduped.api_year
    {% set network_types = dbt_utils.get_column_values(
                           table=ref('xwalk_network_association_types'),
                           column='network_type',
                           where="association_type = 'school'",
                           order_by='network_type')
    %}
    {% if network_types is not none and network_types | length %}
    ,
      {{ dbt_utils.pivot(column='network_type',
                         values=network_types,
                         alias=True,
                         agg='min',
                         prefix='k_network__',
                         then_value='k_network',
                         else_value='null',
                         quote_identifiers=False) }}
  {% endif %}
  from deduped
  join xwalk_network_school_assoc_types xwalk
    on deduped.network_purpose = xwalk.network_purpose
  group by 1,2,3

)

select * from wide