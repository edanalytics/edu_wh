with stage_disabilities as (
	select * from {{ ref('stg_ef3__stu_spec_ed__disabilities') }}
),

primary_disability as (
	select distinct *
	from stg_ef3__stu_spec_ed__disabilities
	where order_of_disability = 1;
)

select * from primary_disability