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

        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__local_accounts', flatten=False) }}

    from stg_local_accounts
)

select * from formatted
