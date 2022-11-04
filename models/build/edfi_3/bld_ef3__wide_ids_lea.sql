with lea_ids as (
    select * from {{ ref('stg_ef3__local_education_agencies__identification_codes') }}
),
xwalk_ids as (
    select * from {{ ref('xwalk_id_types_ed_org') }}
)
select 
    tenant_code,
    api_year,
    k_lea,
    {{ dbt_utils.pivot(column='id_name',
                       values=dbt_utils.get_column_values(
                           table=ref('xwalk_id_types_ed_org'), 
                           column='id_name',
                           order_by='id_name'),
                       alias=True,
                       suffix='_lea_code',
                       agg='min',
                       then_value='id_code',
                       else_value='null',
                       quote_identifiers=False) }}
from lea_ids
join xwalk_ids
    on lea_ids.id_system = xwalk_ids.id_system
group by 1,2,3