{{
    config(
        tags=['finance'],
        post_hook=[
            "alter table {{ this }} alter column k_chart_of_account set not null",
            "alter table {{ this }} add primary key (k_chart_of_account)",
        ]
    )
}}

with chart_of_accounts as (
    select * from {{ ref('stg_ef3__chart_of_accounts') }}
),

balance_sheet_dimensions as (
    select * from {{ ref('stg_ef3__balance_sheet_dimensions') }}
),

fund_dimensions as (
    select * from {{ ref('stg_ef3__fund_dimensions') }}
),

function_dimensions as (
    select * from {{ ref('stg_ef3__function_dimensions') }}
),

object_dimensions as (
    select * from {{ ref('stg_ef3__object_dimensions') }}
),

operational_unit_dimensions as (
    select * from {{ ref('stg_ef3__operational_unit_dimensions') }}
),

program_dimensions as (
    select * from {{ ref('stg_ef3__program_dimensions') }}
),

project_dimensions as (
    select * from {{ ref('stg_ef3__project_dimensions') }}
),

source_dimensions as (
    select * from {{ ref('stg_ef3__source_dimensions') }}
),

formatted as (
    select
        chart_of_accounts.k_chart_of_account,
        chart_of_accounts.k_lea,
        chart_of_accounts.k_school,
        chart_of_accounts.tenant_code,
        chart_of_accounts.chart_of_account_identifier,
        chart_of_accounts.fiscal_year,
        chart_of_accounts.ed_org_id,
        chart_of_accounts.ed_org_type,
        chart_of_accounts.chart_of_account_name,
        chart_of_accounts.account_type,

        balance_sheet_dimensions.balance_sheet_dimension_code,
        balance_sheet_dimensions.balance_sheet_dimension_code_name,

        fund_dimensions.fund_dimension_code,
        fund_dimensions.fund_dimension_code_name,

        function_dimensions.function_dimension_code,
        function_dimensions.function_dimension_code_name,

        object_dimensions.object_dimension_code,
        object_dimensions.object_dimension_code_name,

        operational_unit_dimensions.operational_unit_dimension_code,
        operational_unit_dimensions.operational_unit_dimension_code_name,

        program_dimensions.program_dimension_code,
        program_dimensions.program_dimension_code_name,

        project_dimensions.project_dimension_code,
        project_dimensions.project_dimension_code_name,

        source_dimensions.source_dimension_code,
        source_dimensions.source_dimension_code_name,

        chart_of_accounts.v_reporting_tags,

        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__chart_of_accounts', flatten=False) }}

    from chart_of_accounts
    left join balance_sheet_dimensions
        on chart_of_accounts.k_balance_sheet_dimension = balance_sheet_dimensions.k_balance_sheet_dimension
    left join fund_dimensions
        on chart_of_accounts.k_fund_dimension = fund_dimensions.k_fund_dimension
    left join function_dimensions
        on chart_of_accounts.k_function_dimension = function_dimensions.k_function_dimension
    left join object_dimensions
        on chart_of_accounts.k_object_dimension = object_dimensions.k_object_dimension
    left join operational_unit_dimensions
        on chart_of_accounts.k_operational_unit_dimension = operational_unit_dimensions.k_operational_unit_dimension
    left join program_dimensions
        on chart_of_accounts.k_program_dimension = program_dimensions.k_program_dimension
    left join project_dimensions
        on chart_of_accounts.k_project_dimension = project_dimensions.k_project_dimension
    left join source_dimensions
        on chart_of_accounts.k_source_dimension = source_dimensions.k_source_dimension
)

select * from formatted
