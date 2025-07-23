{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_candidate, k_performance_evaluation, k_person)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_candidate foreign key (k_candidate) references {{ ref('dim_candidate') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_person foreign key (k_person) references {{ ref('dim_candidate') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_performance_evaluation foreign key (k_performance_evaluation) references {{ ref('dim_performance_evaluation') }}",
    ]
  )
}}

with stg_performance_evaluation_ratings as (
    select * from {{ ref('stg_tpdm__performance_evaluation_ratings') }}
),

dim_candidate as (
    select * from {{ ref('dim_candidate') }}
),

dim_performance_evaluation as (
    select * from {{ ref('dim_performance_evaluation') }}
),

formatted as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['dim_candidate.k_candidate',
             'dim_performance_evaluation.k_performance_evaluation',
             'dim_candidate.k_person']
        ) }} as k_candidate_assessment,
        dim_candidate.k_candidate,
        dim_candidate.k_person,
        dim_performance_evaluation.k_performance_evaluation,
        stg_performance_evaluation_ratings.results as rating_results,
        stg_performance_evaluation_ratings.performance_evaluation_rating_level as rating_level,
        stg_performance_evaluation_ratings.reviewers,
        stg_performance_evaluation_ratings.coteaching_style_observed,
        stg_performance_evaluation_ratings.comments,
        stg_performance_evaluation_ratings.is_announced,
        stg_performance_evaluation_ratings.schedule_date,
        stg_performance_evaluation_ratings.actual_date,
        stg_performance_evaluation_ratings.actual_duration,
    from dim_candidate
    join stg_performance_evaluation_ratings
        on dim_candidate.k_person = stg_performance_evaluation_ratings.k_person
    join dim_performance_evaluation
        on stg_performance_evaluation_ratings.k_performance_evaluation = dim_performance_evaluation.k_performance_evaluation
)
select * from formatted