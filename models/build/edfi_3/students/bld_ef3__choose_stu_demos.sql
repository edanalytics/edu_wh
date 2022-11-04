with stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations') }}
),
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
select * from deduped