-- in multi-tenant systems, it is possible for each tenant to individually 
-- define all the districts/schools in a state. We want to avoid having 
-- each tenant individually define each district and school, and only
-- keep the definition from the tenant that actually has ownership
-- since the relationship between tenant and district is not codified anywhere,
-- we observe it from student enrollments.
select distinct ssa.tenant_code, lea_id
from {{ ref('stg_ef3__student_school_associations') }} ssa 
join {{ ref('stg_ef3__schools') }} sch 
    on ssa.k_school = sch.k_school