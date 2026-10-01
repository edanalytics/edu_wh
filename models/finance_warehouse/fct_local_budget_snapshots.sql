{{
    config(
        tags=['finance'],
        post_hook=[
            "alter table {{ this }} alter column k_local_budget_snapshot set not null",
            "alter table {{ this }} add primary key (k_local_budget_snapshot)",
            "alter table {{ this }} add constraint fk_{{ this.name }}_dim_local_account foreign key (k_local_account) references {{ ref('dim_local_account') }}",
        ]
    )
}}

with stg_local_budgets as (
    select * from {{ ref('stg_ef3__local_budgets') }}
),

dim_local_account as (
    select * from {{ ref('dim_local_account') }}
),

with_snapshot_flag as (
    select
        stg_local_budgets.k_local_budget_snapshot,
        stg_local_budgets.k_local_account,
        dim_local_account.k_lea,
        dim_local_account.k_school,
        stg_local_budgets.tenant_code,
        stg_local_budgets.as_of_date,
        stg_local_budgets.local_account_identifier,
        stg_local_budgets.ed_org_id,
        stg_local_budgets.fiscal_year,
        stg_local_budgets.amount,
        stg_local_budgets.financial_collection,
        row_number() over (
            partition by stg_local_budgets.k_local_account
            order by stg_local_budgets.as_of_date desc
        ) = 1 as is_latest_snapshot
    from stg_local_budgets
    join dim_local_account
        on stg_local_budgets.k_local_account = dim_local_account.k_local_account
)

select * from with_snapshot_flag
