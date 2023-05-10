with stage_disabilities as (
	select * from {{ ref('stg_ef3__stu_spec_ed__disabilities') }}
),

primary_disability as (
	select distinct
		tenant_code,
		api_year,
		k_student,
		k_student_year,
		ed_org_id,
		k_lea,
		k_school,
		disability_type,
		disabilitiy_source_type,
		disability_diagnosis,
		order_of_disability,
		disability_designation
	from stg_ef3__stu_spec_ed__disabilities
	where order_of_disability = 1;
)

select * from primary_disability