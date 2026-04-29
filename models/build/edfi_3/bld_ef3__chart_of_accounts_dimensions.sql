{# TODO: decide if this should exist, or if instead we should pull this info out directly from the references
back in base_ef3__chart_of_accounts #}

{{
    config(
        tags=['finance', 'build'],
    )
}}

{#-
  One row per chart of accounts (k_chart_of_account). Left-joins finance dimension
  staging models so display names are in one wide row (linked by k_chart_of_account).

  Expects stg_ef3__chart_of_accounts to carry flattened dimension codes aligned to
  stg_ef3__fund_dimensions, stg_ef3__function_dimensions, stg_ef3__object_dimensions,
  and stg_ef3__source_dimensions.
-#}

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

chart_of_accounts_with_finance_dimensions as (
    select
        chart_of_accounts.tenant_code,
        chart_of_accounts.k_chart_of_account,

        balance_sheet_dimensions.balance_sheet_dimension_code,
        balance_sheet_dimensions.balance_sheet_dimension_fiscal_year,
        balance_sheet_dimensions.balance_sheet_dimension_code_name,

        fund_dimensions.fund_dimension_code,
        fund_dimensions.fund_dimension_fiscal_year, 
        fund_dimensions.fund_dimension_code_name,

        function_dimensions.function_dimension_code,
        function_dimensions.function_dimension_fiscal_year,
        function_dimensions.function_dimension_code_name,

        object_dimensions.object_dimension_code,
        object_dimensions.object_dimension_fiscal_year,
        object_dimensions.object_dimension_code_name,

        operational_unit_dimensions.operational_unit_dimension_code,
        operational_unit_dimensions.operational_unit_dimension_fiscal_year,
        operational_unit_dimensions.operational_unit_dimension_code_name,

        program_dimensions.program_dimension_code,
        program_dimensions.program_dimension_fiscal_year,
        program_dimensions.program_dimension_code_name,

        project_dimensions.project_dimension_code,
        project_dimensions.project_dimension_fiscal_year,
        project_dimensions.project_dimension_code_name,

        source_dimensions.source_dimension_code,
        source_dimensions.source_dimension_fiscal_year,
        source_dimensions.source_dimension_code_name

    from chart_of_accounts
    left join fund_dimensions
        on chart_of_accounts.k_fund_dimension = fund_dimensions.k_fund_dimension
    left join balance_sheet_dimensions
        on chart_of_accounts.k_balance_sheet_dimension = balance_sheet_dimensions.k_balance_sheet_dimension
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

select * from chart_of_accounts_with_finance_dimensions
