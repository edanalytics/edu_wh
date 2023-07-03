with stage_disabilities as (
	select * from {{ ref('stg_ef3__stu_spec_ed__disabilities') }}
),

primary_disability as (
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
)

select * from primary_disability