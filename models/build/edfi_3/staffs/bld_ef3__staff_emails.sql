-- combine staff emails from multiple sources in ed-fi
with staff_email as (
    select
        tenant_code,
        k_staff,
        null as k_lea,
        null as k_school,
        email_type,
        email_address
    from {{ ref('stg_ef3__staffs__emails') }}
),
staff_edorg_email as (
    select
        seoca.tenant_code,
        seoca.k_staff,
        seoca.k_lea,
        seoca.k_school,
        seoca.contact_title as email_type,
        seoca.email_address
    from {{ ref('stg_ef3__staff_education_organization_contact_associations') }} as seoca
    -- use staffs join to filter to the last observed school_year for a given staff contact record
    join {{ ref('stg_ef3__staffs') }} as staffs
        on seoca.k_staff = staffs.k_staff
        and seoca.api_year = staffs.api_year
),
stacked as (
    select * from staff_email
    union all
    select * from staff_edorg_email
)
select
    *,
    -- allow dots, hyphens, and underscores in email (and optionally plus-addressing)
    -- but don't allow apostrophes, spaces, other characters
    -- allow final URL component to be between 2 and 9 characters
    email_address rlike '^[a-zA-Z0-9_.-]+[+]?[a-zA-Z0-9.-]*@[a-zA-Z0-9.-]+[.][a-zA-Z0-9]{2,9}$' as is_valid_email
from stacked
