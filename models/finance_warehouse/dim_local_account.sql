{{
    config(
        tags=['finance'],
        post_hook=[
            "alter table {{ this }} alter column k_local_account set not null",
            "alter table {{ this }} add primary key (k_local_account)",
        ]
    )
}}

with stg_local_accounts as (
    select * from {{ ref('stg_ef3__local_accounts') }}
),

bld_chart_of_accounts_dimensions as (
    select * from {{ ref('bld_ef3__chart_of_accounts_dimensions') }}
),

formatted as (
    select
        stg_local_accounts.k_local_account,
        stg_local_accounts.k_chart_of_account,
        stg_local_accounts.k_lea,
        stg_local_accounts.k_school,
        stg_local_accounts.tenant_code,
        stg_local_accounts.local_account_identifier,
        stg_local_accounts.fiscal_year,
        stg_local_accounts.ed_org_id,
        stg_local_accounts.ed_org_type,
        stg_local_accounts.local_account_name,
        stg_local_accounts.v_reporting_tags,
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
        bld_chart_of_accounts_dimensions.program_dimension_fiscal_year,
        bld_chart_of_accounts_dimensions.program_dimension_code_name,
        bld_chart_of_accounts_dimensions.project_dimension_code,
        bld_chart_of_accounts_dimensions.project_dimension_fiscal_year,
        bld_chart_of_accounts_dimensions.project_dimension_code_name,
        bld_chart_of_accounts_dimensions.source_dimension_code,
        bld_chart_of_accounts_dimensions.source_dimension_fiscal_year,
        bld_chart_of_accounts_dimensions.source_dimension_code_name,

        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__local_accounts', flatten=False) }}

    from stg_local_accounts
    left join bld_chart_of_accounts_dimensions
        on stg_local_accounts.k_chart_of_account = bld_chart_of_accounts_dimensions.k_chart_of_account

)

select * from formatted
