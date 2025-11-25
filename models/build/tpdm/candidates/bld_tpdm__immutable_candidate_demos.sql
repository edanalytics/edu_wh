{# If edu var has been configured to make demos immutable, set partition var to `k_candidate_xyear` so demos are unique by xyear #}
{# otherwise, use k_candidate so demos are unique by candidate+year #}
{%- if var('edu:candidate_demos:make_demos_immutable', False) -%}
    {%- set candidate_partition_var = 'k_candidate_xyear' -%}
{%- else -%}
    {%- set candidate_partition_var = 'k_candidate' -%}
{%- endif -%}

-- extract the most recent version of immutable candidate demographics
-- so that our most current understanding of these values can be applied
-- accross all historic years
with stg_candidate as (
    select * from {{ ref('stg_tpdm__candidates') }}
),
candidate_races as (
    select * from {{ ref('bld_tpdm__candidate_race_ethnicity') }}
),
joined as (
    select
        stg_candidate.k_candidate,
        stg_candidate.k_candidate_xyear,
        stg_candidate.tenant_code,
        stg_candidate.school_year,
        stg_candidate.first_name,
        stg_candidate.middle_name,
        stg_candidate.last_name,
        {# candidate_display_name logic: prefer SQL from this dbt variable, but default to "concat(...)" #}
        {{ var('edu:candidate_demos:display_name_sql',
          "concat(
            stg_candidate.last_name, ', ',
            stg_candidate.first_name,
            coalesce(' ' || left(stg_candidate.middle_name, 1), '')
            )"
          )
        }} as display_name,
        concat(display_name, ' (', stg_candidate.candidate_id, ')') as safe_display_name,
        stg_candidate.maiden_name,
        stg_candidate.personal_title_prefix,
        stg_candidate.preferred_first_name,
        stg_candidate.preferred_last_name,
        stg_candidate.gender,
        candidate_races.race_ethnicity,
        candidate_races.race_array,
        candidate_races.has_hispanic_latino_ethnicity
    from stg_candidate
    left join candidate_races
        on stg_candidate.k_candidate = candidate_races.k_candidate
),
deduped as (
        {{
        dbt_utils.deduplicate(
            relation='joined',
                partition_by=candidate_partition_var,
            order_by='school_year desc'
        )
    }}
)
select * from deduped