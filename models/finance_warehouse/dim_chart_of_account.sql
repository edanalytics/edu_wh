{{
    config(
        tags=['finance'],
        post_hook=[
            "alter table {{ this }} alter column k_chart_of_account set not null",
            "alter table {{ this }} add primary key (k_chart_of_account)",
        ]
    )
}}

with stg_chart_of_accounts as (
    select * from {{ ref('stg_ef3__chart_of_accounts') }}
),

bld_chart_of_accounts_dimensions as (
    select * from {{ ref('bld_ef3__chart_of_accounts_dimensions') }}
),

formatted as (
    select
        stg_chart_of_accounts.k_chart_of_account,
        stg_chart_of_accounts.k_lea,
        stg_chart_of_accounts.k_school,
        stg_chart_of_accounts.tenant_code,
        stg_chart_of_accounts.chart_of_account_identifier,
        stg_chart_of_accounts.fiscal_year,
        stg_chart_of_accounts.ed_org_id,
        stg_chart_of_accounts.ed_org_type,
        stg_chart_of_accounts.chart_of_account_name,
        stg_chart_of_accounts.account_type,
        {# TODO these seem unnecessary.. do we add? #}
        {# stg_chart_of_accounts.k_balance_sheet_dimension,
        stg_chart_of_accounts.k_fund_dimension,
        stg_chart_of_accounts.k_function_dimension,
        stg_chart_of_accounts.k_object_dimension,
        stg_chart_of_accounts.k_operational_unit_dimension,
        stg_chart_of_accounts.k_program_dimension,
        stg_chart_of_accounts.k_project_dimension,
        stg_chart_of_accounts.k_source_dimension, #}
        bld_chart_of_accounts_dimensions.balance_sheet_dimension_code,
        bld_chart_of_accounts_dimensions.balance_sheet_dimension_fiscal_year,
        bld_chart_of_accounts_dimensions.balance_sheet_dimension_code_name,
        bld_chart_of_accounts_dimensions.fund_dimension_code,
        bld_chart_of_accounts_dimensions.fund_dimension_fiscal_year,
        bld_chart_of_accounts_dimensions.fund_dimension_code_name,
        bld_chart_of_accounts_dimensions.function_dimension_code,
        bld_chart_of_accounts_dimensions.function_dimension_fiscal_year,
        bld_chart_of_accounts_dimensions.function_dimension_code_name,
        bld_chart_of_accounts_dimensions.object_dimension_code,
        bld_chart_of_accounts_dimensions.object_dimension_fiscal_year,
        bld_chart_of_accounts_dimensions.object_dimension_code_name,
        bld_chart_of_accounts_dimensions.operational_unit_dimension_code,
        bld_chart_of_accounts_dimensions.operational_unit_dimension_fiscal_year,
        bld_chart_of_accounts_dimensions.operational_unit_dimension_code_name,
        bld_chart_of_accounts_dimensions.program_dimension_code,
        bld_chart_of_accounts_dimensions.program_dimension_code_name,
        bld_chart_of_accounts_dimensions.project_dimension_code,
        bld_chart_of_accounts_dimensions.project_dimension_fiscal_year,
        bld_chart_of_accounts_dimensions.project_dimension_code_name,
        bld_chart_of_accounts_dimensions.source_dimension_code,
        bld_chart_of_accounts_dimensions.source_dimension_code_name,
        bld_chart_of_accounts_dimensions.source_dimension_fiscal_year,

        stg_chart_of_accounts.v_reporting_tags,


        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__chart_of_accounts', flatten=False) }}

    from stg_chart_of_accounts
    join bld_chart_of_accounts_dimensions
        on stg_chart_of_accounts.k_chart_of_account = bld_chart_of_accounts_dimensions.k_chart_of_account
)


select * from formatted
