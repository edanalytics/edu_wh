with all_tests_rows as (
    select *,
        parse_json(result_row) as v_result_row
    from {{ ref('test_result_rows') }}
),
all_tests as (
    select *
    from {{ ref('elementary_test_results') }}
    -- temporary filter to include only new tests generated from this PR
    where detected_at > '2025-04-08 20:58:22.000'
    and test_sub_type = 'generic'

)
,
stacked as (
    select all_tests.*,
        all_tests_rows.v_result_row
    from all_tests
    inner join all_tests_rows
    on all_tests.id = all_tests_rows.elementary_test_results_id


)
select id, 
    detected_at,
    table_name,
    test_sub_type as test_type,
    test_short_name as test_category,
    v_result_row:test_params::varchar as test_params,
    coalesce(v_result_row:api_year::varchar, null) as api_year,
    v_result_row:tenant_code::varchar as tenant_code,
    status,
    v_result_row:failed_row_count::numeric as failed_row_count
from stacked
group by all