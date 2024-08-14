with stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations') }}
),
--note: in most implementations, student_education_organization_association is already unique by k_student 
-- so this model effectively does nothing. There are two cases where this model has an impact:
-- 1. demographics are set at the school level rather than the district level. We have never seen a case where this is true.
-- 2. Multiple districts are combined in a single ODS, such as a state implementation. In this case setting the variable below 
-- will cause us to choose the demographics from the district in which they were most recently enrolled. We avoid doing this in other cases
-- to avoid the additional expense and complexity
{% if {{ var('edu:combined_district_ods', false) }} %}
choose_latest_enrollment as (
    select k_student, k_lea 
    from {{ ref('fct_student_school_association') }}
    qualify 1 = row_number() over (partition by k_student order by exit_withdraw_date desc, entry_date desc)
),
deduped as (
    select stu_ed_org.*
    from stu_ed_org
    join choose_latest_enrollment
        on stu_ed_org.k_student = choose_latest_enrollment.k_student
        and stu_ed_org.k_lea    = choose_latest_enrollment.k_lea
)
{% else %}
deduped as (
    -- select a single representative set of demographics,
    -- since foreign keys from other tables do _not_ distinguish by ed-org
    -- reliably
        {{
        dbt_utils.deduplicate(
            relation='stu_ed_org',
            partition_by='k_student',
            order_by='ed_org_type, ed_org_id'
        )
    }}
)
{% endif %}
select * from deduped
