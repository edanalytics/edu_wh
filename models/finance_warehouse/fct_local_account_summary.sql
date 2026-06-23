{{
    config(
        tags=['finance'],
        post_hook=[
            "alter table {{ this }} alter column k_local_account set not null",
            "alter table {{ this }} add primary key (k_local_account)",
        ]
    )
}}

with dim_local_account as (
    select * from {{ ref('dim_local_account') }}
),

dim_chart_of_account as (
    select * from {{ ref('dim_chart_of_account') }}
),

latest_actuals as (
    select * from {{ ref('fct_local_actual_snapshots') }}
    where is_latest_snapshot
),

latest_budgets as (
    select * from {{ ref('fct_local_budget_snapshots') }}
    where is_latest_snapshot
),

summary as (
    select
        dim_local_account.k_local_account,
        dim_local_account.k_chart_of_account,
        dim_local_account.k_lea,
        dim_local_account.k_school,
        dim_local_account.tenant_code,
        dim_local_account.local_account_identifier,
        dim_local_account.fiscal_year,
        dim_local_account.ed_org_id,
        dim_local_account.ed_org_type,
        dim_local_account.local_account_name,

        dim_chart_of_account.chart_of_account_identifier,
        dim_chart_of_account.account_type,
        dim_chart_of_account.balance_sheet_dimension_code,
        dim_chart_of_account.balance_sheet_dimension_code_name,
        dim_chart_of_account.fund_dimension_code,
        dim_chart_of_account.fund_dimension_code_name,
        dim_chart_of_account.function_dimension_code,
        dim_chart_of_account.function_dimension_code_name,
        dim_chart_of_account.object_dimension_code,
        dim_chart_of_account.object_dimension_code_name,
        dim_chart_of_account.operational_unit_dimension_code,
        dim_chart_of_account.operational_unit_dimension_code_name,
        dim_chart_of_account.program_dimension_code,
        dim_chart_of_account.program_dimension_code_name,
        dim_chart_of_account.project_dimension_code,
        dim_chart_of_account.project_dimension_code_name,
        dim_chart_of_account.source_dimension_code,
        dim_chart_of_account.source_dimension_code_name,

        latest_actuals.amount as actual_amount,
        latest_actuals.as_of_date as actual_as_of_date,
        latest_actuals.financial_collection as actual_financial_collection,

        latest_budgets.amount as budget_amount,
        latest_budgets.as_of_date as budget_as_of_date,
        latest_budgets.financial_collection as budget_financial_collection

    from dim_local_account
    left join dim_chart_of_account
        on dim_local_account.k_chart_of_account = dim_chart_of_account.k_chart_of_account
    left join latest_actuals
        on dim_local_account.k_local_account = latest_actuals.k_local_account
    left join latest_budgets
        on dim_local_account.k_local_account = latest_budgets.k_local_account
)

select * from summary
