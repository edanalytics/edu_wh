-- in multi-tenant systems, it is possible for each tenant to individually 
-- define all the districts/schools in a state. We want to avoid having 
-- each tenant individually define each district and school, and only
-- keep the definition from the tenant that actually has ownership
-- since the relationship between tenant and district is not codified anywhere,
-- we observe it from calendars
select distinct sch.tenant_code, sch.lea_id
from {{ ref('stg_ef3__calendars') }} as cal
join {{ ref('stg_ef3__schools') }} as sch
    on cal.k_school = sch.k_school
