with stg_stu_address as (
    select * from {{ ref('stg_ef3__stu_ed_org__addresses') }}
),
stg_stu_emails as (
    select * 
    from {{ ref('stg_ef3__stu_ed_org__emails') }}
), 
stg_stu_phones as (
    select * 
    from {{ ref('stg_ef3__stu_ed_org__telephones') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
)
select 
    dim_student.k_student, 
    dim_student.k_student_xyear,
    dim_student.tenant_code, 
    dim_student.school_year,
    address_type,
    street_address,
    city,
    name_of_county,
    state_code,
    postal_code,
    building_site_number,
    locale,
    congressional_district,
    county_fips_code,
    latitude,
    longitude,
    email_type, 
    email_address,
    phone_number_type, 
    phone_number
from dim_student 
left join stg_stu_address on dim_student.k_student = stg_stu_address.k_student 
left join stg_stu_emails  on dim_student.k_student = stg_stu_emails.k_student 
left join stg_stu_phones  on dim_student.k_student = stg_stu_phones.k_student
