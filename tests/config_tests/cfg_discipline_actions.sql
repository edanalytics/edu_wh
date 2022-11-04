{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with stg_discipline_actions as (
    select * from {{ ref('stg_ef3__discipline_actions') }}
),
xwalk_discipline_actions as (
    select * from {{ ref('xwalk_discipline_actions') }}
),
flattened as (
    --todo: duplicated logic. consider splitting to build table
    select *, {{ dbt_edfi_source.extract_descriptor('value:disciplineDescriptor::string') }} as discipline_action
    from stg_discipline_actions
        , lateral flatten(v_disciplines)
),
joined as (
    select distinct
        flattened.tenant_code,
        flattened.api_year,
        flattened.discipline_action
    from flattened
    left join xwalk_discipline_actions
        on flattened.discipline_action = xwalk_discipline_actions.discipline_action
    where xwalk_discipline_actions.is_oss is null
)
select * from joined