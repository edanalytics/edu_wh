with stage_disabilities as (
	select * from {{ ref('stg_ef3__stu_spec_ed__disabilities') }}
),

filtered as (
	select
		tenant_code,
		school_year,
		k_student,
		k_student_xyear,
		k_program,
		k_lea,
		k_school,
		program_enroll_begin_date,
		disability_type,
		disability_source_type,
		disability_diagnosis,
		order_of_disability,
		disability_designation
	from stage_disabilities
	where order_of_disability = 1
),

deduped as (
	--this order_by is not substantive, but is necessary for consistency
	{{
		dbt_utils.deduplicate(
			relation='filtered',
			partition_by='k_student, k_program, school_year, program_enroll_begin_date',
			order_by='disability_type, disability_designation'
		)
	}}
)

select * from deduped