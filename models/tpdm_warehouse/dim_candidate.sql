with stg_candidates as (
    select * from {{ ref('stg_tpdm__candidates') }}
),

formatted as (
    select
        k_candidate,
        k_person, 
        tenant_code,
        api_year,
        candidate_id,
        person_id, 
        first_name, 
        last_name, 
        middle_name, 
        maiden_name, 
        generation_code_suffix, 
        personal_title_prefix, 
        preferred_first_name, 
        preferred_last_name, 
        birth_city, 
        birth_date, 
        birth_international_province, 
        date_entered_us, 
        displacement_status, 
        is_economic_disadvantaged, 
        is_first_generation_student, 
        has_hispanic_latino_ethnicity, 
        is_multiple_birth, 
        gender, 
        sex, 
        birth_sex, 
        birth_state, 
        birth_country, 
        english_language_exam, 
        lep_code, 
        v_addresses, 
        v_disabilities, 
        v_emails, 
        v_languages, 
        v_other_names, 
        v_personal_identification_documents, 
        v_races, 
        v_telephones 
    from stg_candidates
)

select * from formatted