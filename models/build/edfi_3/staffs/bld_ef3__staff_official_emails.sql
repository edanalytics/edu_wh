{{
  config(
    materialized = 'view',
    )
}}

{% set work_email_codes = var('edu:staff:official_email_codes', ['Work']) %}
{% if work_email_codes is string %} {% set work_email_codes = [work_email_codes] %} {% endif %}

{% set banned_personal_domains = var('edu:staff:banned_personal_domains', 
    ['aim.com', 'aol.com', 'att.net', 'gmail.com', 'yahoo.com', 
    'hotmail.com', 'msn.com', 'live.com', 'charter.net', 
    'earthlink.net', 'verizon.net', 'comcast.net', 'outlook.com']) %}
{% if banned_personal_domains is string %} {% set banned_personal_domains = [banned_personal_domains] %}{% endif %}

-- keep only emails that are valid, coded as a 'work'-type email, and don't appear to be personal domains
with official_emails as (
    select *
    from {{ ref('bld_ef3__staff_emails') }}
    where is_valid_email
        and email_type in ('{{ work_email_codes | join("', '") }}')
        and split_part(email_address, '@', 2) not in ('{{ banned_personal_domains | join("', '") }}')
),
-- keep only one row per k_staff and unique email address, regardless of where it came from
-- sort is arbitrary, since source doesn't really matter here
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='official_emails',
            partition_by='k_staff, email_address',
            order_by='email_type'
        )
    }}
)
select * from deduped
