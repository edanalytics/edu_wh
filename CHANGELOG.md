# Unreleased
## New features
## Under the hood
## Fixes
- Fix `period_duration` calculation in `dim_class_period` to account for different time formats.

# edu_wh v0.6.0
## New features
- Add `attendance_excusal_status` and `consecutive_days_by_excusal_status` to `fct_student_daily_attendance`, where:
    - `attendance_excusal_status` classifies each attendance record as 'In Attendance', 'Not Enrolled', 'Excused Absence', or 'Unexcused Absence'
    - `consecutive_days_by_excusal_status` reports the number of consecutive days a student has had the same attendance_excusal_status
- Add `attendance_excusal_status`and `calendar_date` to `fct_student_school_attendance_event`
- Add TPDM/EPDM domain warehouse models: `dim_educator_preparation_program`, `fct_candidate_educator_preparation_program`, `fct_candidate_staff_association`
- Add "bridge table" `brg_course_section_program` for convenient linking between course sections & programs
## Under the hood
- Update syntax of dbt test argument declarations to avoid deprecated behavior. Requires dbt 1.10.5+
## Fixes
## Migrations
- see Release page

# edu_wh v0.5.3
## New features
- Add `dim_candidate` model
## Fixes
- Minor fixes for Databricks compatibility

# edu_wh v0.5.2
## New features
- Add `safe_display_name` to `dim_staff`, the logic for this column replicates that of `bld_ef3__immutable_stu_demos`.
## Fixes
- Updated tests `dbt_utils_unique_combination_of_columns_fct_student_language_instruction_program_association_k_student__k_program`, `dbt_utils_unique_combination_of_columns_fct_student_program_association_k_student__k_program`, and `dbt_utils_unique_combination_of_columns_fct_student_title_i_part_a_program_association_k_student__k_program` to include two additional columns. Previous test only listed partial primary key. 

# edu_wh v0.5.1
## New features
- Add fct models `fct_student_cte_program_associations`, `fct_student_migrant_education_program_associations`, and `fct_student_school_food_service_program_associations`
- Add `bld_ef3__student__other_names` and conditional code in `dim_student` to pull into columns, if configured in dbt var `'edu:stu_demos:other_names'`.
- Add tests `sections_without_staff`, `sections_without_students`, `enrollments_without_overlapping_sections`, and `schools_with_enrollments_without_overlapping_sections` to test for rostering data issues.
- Add QC model `sections_per_enrollment` to assist with identifying school enrollments potentially missing corresponding section enrollment data.
## Under the hood
- Update join logic in `bld_ef3__student_assessments_long_results` and `cfg_assessment_scores` to  join on the `assessment_family` and/or `assessment_identifier` fields in `xwalk_assessment_scores`, if they have been provided. This allows for score configuration by either assess ID or family
## Fixes
- Fix model `fct_student_daily_attendance` to prevent incorrect 100% attendance rates in prior years. Includes `school_year` in `school_max_submitted.max_date_by_school`.
## Migration
- (Optional) Configure `xwalk_assessment_scores`. Add in `assessment_family` field and remove redundant records.

# edu_wh v0.5.0
## New features
- Add Databricks platform compatibility
- Add optional `gender_identity` to `dim_student` as an immutable demographic. This field was introduced to Ed-Fi in Data Standard v5
- Add optional DS5 fields to `fct_student_school_association`: `is_school_choice`, `school_choice_basis`, `enrollment_type`, `next_year_school_id`, `next_year_grade_level`
## Under the hood
- The following 'breaking' under the hood changes were introduced for databricks compatibility:
  - All columns which are part of the primary key of a table are set explicitly as not null
  - The primary key of `fct_student_gpa` has been changed to remove is_cumulative, but the logic was adjusted so that the effective grain is the same
  - Added `fct_staff_school_association.k_staff_school_association` and updated the primary key
  - Changed column order in `fct_student_assessment` and `fct_student_objective_assessment`
  - Changed `dim_class_period.start_time` and `end_time` from time data types to strings
## Fixes
- Potentially breaking for queryers: `bld_ef3__combine_gpas` (and downstream `fct_student_gpa`) `gpa_type` `'Unknown'` values have been made more specific: `'Cumulative, unknown weighting'` and `'Non-cumulative, unknown weighting'` to respect the grain of the table.

# edu_wh v0.5.0
## New features
- Add Databricks platform compatibility
- Add optional `gender_identity` to `dim_student` as an immutable demographic. This field was introduced to Ed-Fi in Data Standard v5
- Add optional DS5 fields to `fct_student_school_association`: `is_school_choice`, `school_choice_basis`, `enrollment_type`, `next_year_school_id`, `next_year_grade_level`
## Under the hood
- The following 'breaking' under the hood changes were introduced for databricks compatibility:
  - All columns which are part of the primary key of a table are set explicitly as not null
  - The primary key of `fct_student_gpa` has been changed to remove is_cumulative, but the logic was adjusted so that the effective grain is the same
  - Added `fct_staff_school_association.k_staff_school_association` and updated the primary key
  - Changed column order in `fct_student_assessment` and `fct_student_objective_assessment`
  - Changed `dim_class_period.start_time` and `end_time` from time data types to strings
## Fixes
- Potentially breaking for queryers: `bld_ef3__combine_gpas` (and downstream `fct_student_gpa`) `gpa_type` `'Unknown'` values have been made more specific: `'Cumulative, unknown weighting'` and `'Non-cumulative, unknown weighting'` to respect the grain of the table.

# edu_wh v0.4.4
## New features
- Add `birth_country` to `bld_ef3__immutable_stu_demos` and upstream in `dim_student`
- Add `bld_ef3__staff_emails`, a build model that combines staff emails from `stg_ef3__staffs__emails` and `stg_ef3__staff_education_organization_contact_associations`
- Add `bld_ef3__staff_official_emails`, a build model that filters staff emails to official/work emails (can be used for RLS)
## Under the hood
- Change dim_staff email sourcing to this new combined model ^ `bld_ef3__staff_emails`
- Update dim_student.yaml (dbt documentation) to be more precise on surrogate key defs for commonly referenced k_student and k_student_xyear

# edu_wh v0.4.3
## Under the hood
- Add `incident_id_array` to fct_student_discipline_actions_summary
- Add `begin_date` to primary key of `fct_staff_section_associations` to align with DS 5.0 and later
- Add `school_year` to `dim_discipline_incident`
## Fixes
- Fix model `dim_course` to handle multiple academic subjects per course (Ed-Fi Data Standard v5.0 breaking change)
- Add array column `subject_array` to `dim_course`, containing array of academic subjects if these exist
- Add logic to populate `academic_subject` column with single-valued subjects in both cases where data source is <5.0 or >5.0
- Add upstream `bld_ef3__course_subject`

# edu_wh v0.4.2
## New features
- Add tests `cfg_assessment_scores` and `cfg_objective_assessment_scores` to find assess/obj assess with no scores configured
- Add support for custom indicators on `dim_school`

# edu_wh v0.4.1
## New features
- Add `v_credit_categories` to `fct_course_transcripts` for use in credit calcuations
- Add support for custom indicators on `dim_staff`
- Add support for custom indicators on `dim_calendar_date`
## Under the hood
- Tweak tenant-lea attribution logic to use calendars instead of student-school-associations (preferred behavior for beginning of school year)
## Fixes
- Fix model `fct_student_school_attendance_event` to account for the case where a student has multiple enrollments with different calendars at the same school
- Fix configuration header in tests/_...yml files to remove warning introduced by dbt 1.9.0

# edu_wh v0.4.0
## New features
- Add array column `cohort_year_array` to `dim_student`, tracking student cohort designation, and add upstream `bld_ef3__student_cohort_years`
- Add support for custom indicators on `dim_course_section`, and companion audit table for testing uniqueness of custom data sources
- Add `section_type` descriptor column to `dim_section` (Ed-Fi Data Standard v5.0 addition)
- Add `preferred_first_name`, `preferred_last_name`, and `gender_identity` columns to `dim_staff` (Ed-Fi Data Standard v5.0 additions)
- Add `preferred_first_name`, `preferred_last_name`, and `gender_identity` columns to `dim_parent` (Ed-Fi Data Standard v5.0 additions)
## Under the hood
- Change the source of `dim_parent` to `stg_ef3__contacts` and `fct_student_parent_association` to `stg_ef3__student_contact_associations` due to the rename from parent to contact in Ed-Fi data standard v5.0.
- Add additional foreign key declarations to `fct_student_discipline_actions`, `fct_student_discipline_actions_summary`, `fct_student_discipline_incident_behaviors`
- Update package dependency `dbt_utils` to 1.3.0, including alignment to renamed `generate_surrogate_key()` macro. Note, this change now treats nulls and empty strings as distinct values in surrogate key generation.
## Fixes
- Fix model name in yaml documentation file for `dim_graduation_plan`
- Fix unique key test for recently changed unique key fo `fct_student_school_attendance_event`

# edu_wh v0.3.4
## Fixes
- Fix `bld_ef3__wide_school_network_assoc` to group across years, to correctly remove duplicates on `k_school`. Previously, incorrect duplicate records were created in `dim_school` in cases where multiple network types are configured in `xwalk_network_association_types`.

# edu_wh v0.3.3
## New features
- Add `fct_student_diploma` and a companion test for monitoring deduplicated data - `diploma_record_duplicates`
- Add `dim_graduation_plan` and reference via `fct_student_school_association.k_graduation_plan`
- Add some notes to dbt docs for `fct_student_special_education_program_association` and `fct_student_section_association`
- Add `v_earned_additional_credits` to `fct_course_transcript`
## Fixes
- Modify the join in `bld_ef3__stu_race_ethnicity` so that students with unknown race are included and `{{ var("edu:stu_demos:race_unknown_code") }}` is correctly applied
## Under the hood
- Rework and rename pivot macro to `ea_pivot()` to simplify usage
- Add `k_lea` and `k_school` to `dim_course`. Note - downstream queries that reference `k_lea` or `k_school` without an explicit qualified column reference may break due to this change.
- Add macro call that brings through extensions to all fct tables that directly reference a stg table. See [here](https://github.com/edanalytics/edu_wh/blob/124636845754dbcde89ebcfea2c39dfa8b1679b0/models/core_warehouse/fct_course_transcripts.sql#L50) for example.
    - Note: this may break in certain edge cases, if your implementation has existing configured extensions whose names collide with column names that already exist in the related fct table. This should be rare.
- Add extension columns (optional, if configured) to all fct tables. If no extensions configured, this code compiles to nothing.


# edu_wh v0.3.2
## New features
- Add `has_hispanic_latino_ethnicity` to `dim_student`. Also include in this in fields that are "immutable" (consistent across years), assuming variable `edu:stu_demos:make_demos_immutable` is set to `True`.
- Add configurable student language columns to `dim_student`.
- Add configurable custom override for student grade level (some source other than student-school-assoc in Ed-Fi). Use variable `edu:stu_demos:grade_level_override` to configure a data source and column.
- Add configurable logic for override of `school_year` in `fct_student_assessment` and `fct_student_objective_assessment` (e.g. if NULL, use thresholds to populate). By default, no override is done, but options were added to use a global variable `edu:school_year:start_month`, `edu:school_year:start_day`, or use a xwalk of date ranges `xwalk_assessment_school_year_dates`.
- Make `stu_display_name` configurable (e.g. include suffix or preferred_name)
## Under the hood
- Correct dbt docs for unique key of `fct_student_daily_attendance`.
- Add `k_school` to grain of qc model `attendance_freshness`.
  - Also restrict `attendance_freshness` model to current or past dates
  - And add column `days_since_last_attendance_event` to `attednance_freshness_test`
## Fixes
- Remove `is_latest_record` from auto-creation of subgroups for `dim_subgroup` and `fct_student_subgroup`.

# edu_wh v0.3.1
## New features
- Add `dim_staff.race_ethnicity`, using rules analogous to `dim_student.race_ethnicity`


# edu_wh v0.3.0
## New features
- Standardize ESC naming and add `k_esc` reference to `dim_lea`
- Add integer grade level to `dim_student` and `fct_student_school_association`
- Add `k_student_xyear` to `fct_student_assessment`, `fct_course_transcripts`, `fct_student_academic_record`, and `fct_student_gpa`.
- Discipline updates:
   - Add ability to report all disciplines/behaviors for a single discipline action event and all disciplines/behaviors for a single discipline incident in new, separated models.
   - See more info in PR description [#75](https://github.com/edanalytics/edu_wh/pull/75/files)
- Add some comments for existing value tests
- Add feature to conditionally remove xyear enrollments in `fct_student_school_association`
## Under the hood
- Make `k_student` null where not joinable to `dim_student`, for  `fct_student_assessment`, `fct_course_transcripts`, `fct_student_academic_record`, and `fct_student_gpa`.
## Fixes
- Fix unique key of `fct_student_school_attendance_event` and `fct_student_section_attendance_event` to reflect unique key in Ed-Fi.
   - Note: The unique key of `fct_student_daily_attendance` remains the same `(k_student, k_school, calendar_date)`. But the way we deduplicate to this point has changed to be more consistent
   - The scale of impact of this fix depends on how often duplicates are found on `(k_student, k_school, calendar_date)` in the underlying data. See new test `analytics.prod_dbt_test__audit.attendance_event_duplicates` for more information.
## Migration
- Update packages.yml version range for edu_wh to `[ ">=0.3.0", "<0.4.0" ]`
- Configure xwalk_grade_levels. Can copy directly from project template [here](https://github.com/edanalytics/edu_project_template/blob/main/dbt/seeds/xwalk_grade_levels.csv),
  but also check your implementation's grade level descriptors ```select * from analytics.prod_stage.int_ef3__deduped_descriptors
  where namespace ilike '%GradeLevelDescriptor%';```. If this shows custom/local descriptors, make sure to add those to the xwalk.
   - After running the xwalk, you can also do a check for missing values with ```select * from analytics.prod_wh.dim_student where grade_level_integer is null;```
- (requires Snowflake sysadmin or transformer_prod permissions) Drop the following deprecated tables:
   - `analytics.prod_wh.dim_education_service_center`
   - `analytics.prod_wh.dim_discipline_incidents`
   - `analytics.prod_wh.fct_student_discipline_incident_behaviors_actions`


# edu_wh v0.2.10
## Fixes
- Bugfix release

# edu_wh v0.2.9
## New features
- Add ability to extend `dim_course` and `dim_class_period` with external data sources
- Add an `is_latest_record` indicator to `dim_student` to identify the demographics from the most recent school year in which a student appeared
## Under the hood
- In `dim_student`, choose the grade level from the most recent school enrollment, rather than the longest duration. This better aligns with grade transition patterns seen in the wild.
- Deprecate the `rls` schema in favor of extensions that cover this use-case

# edu_wh v0.2.8
## New features
- Add `dim_learning_standard` and `fct_student_learning_standard_grades`
    - For Learning Standard based grading, and for future support of learning standards in other areas of the warehouse
- Add `dim_cohort` and `fct_student_cohort_association`
    - Not widely in use, but intended to support more complex authorization models in the future
## Under the hood
- Allow overriding the source of daily attendance models, for more complex customizations of the daily attendance calculation


# edu_wh v0.2.7
## New features
- Add incident_time to dim_discipline_incidents
- Allow some student demographics to be 'immutable', in the sense that they are stable across time
- Allow custom indicators on program memberships
## Under the hood
- Make it optional to exclude student-school enrollments where the student exited before the first day of school
- Allow a rule where multiple calendar codes must be assigned to a date for it to be counted as a school day



# edu_wh v0.2.6
## New features
- Add primary disability on student special education program associations
- Documentation improvements

# edu_wh v0.2.5
## New features
- Create wide indicators from course/section characteristics in `dim_course_section`
## Under the hood
- Add defaults for discipline non-offender codes
- Minor tweaks to documentation, configuration
- Allow assessment results to come through even if they don't match a xwalk entry
- Add `k_school` to more models
- Add configuration to customize the preferred email address for staff
- Restrict the relationships between tenant and LEAs/schools

# edu_wh v0.2.4
## New features
- New models covering the relationship between discipline incidents, actions, and behaviors
- Extend documentation coverage
## Under the hood
- accordion_columns gains ability to coalesce null values with a default
- Fill False instead of NULL in some student demographics
## Migration
- Configure non-offender codes. This allows for navigating the Ed-Fi deprecation
    of the old student discipline model. The new model separates offenders from
    non-offendors, and this config allows us to apply the same logic to the
    now-deprecated model. Any `DisciplineIncidentParticipationCodeDescriptors`
    in use in `studentDisciplineIncidentAssociations` that refer to non-offenders
    should be included in this list. See [here](https://github.com/edanalytics/edu_project_template/blob/d58d7ffd95cfe113852a15e5f724f9641363a593/dbt/dbt_project.yml#L58)
- New crosswalk for ranking the severity of Behaviors (template [here](https://github.com/edanalytics/edu_project_template/blob/main/dbt/seeds/xwalk_discipline_behaviors.csv)). This
    facilitates the analysis of incidents involving multiple behaviors.
- `fct_discipline_actions` has been renamed to `fct_student_discipline_actions`. Any references to this model will need to be updated.

# edu_wh v0.2.3
## New features
- Add models for education service centers
- Add support for custom indicators on dim_school
- More support for generic student programs
## Under the hood
- More robust creation of first/last day of school table for calendaring/enrollment logic
- Generate foreign keys on optional school-network associations for BI tools
## Fixes
- Implement "first day school" rule for single-calendar schools in fct_student_school_association
- Allow extension columns to correctly be pulled into the stacked fct_program_service table
- Fix join between staff assignments and staff-school associations

# edu_wh v0.2.2
## Fixes
- Bugfix release: package specification

# edu_wh v0.2.1
## New features
- Add fact table for student GPAs
- Add additional program-type fact tables
- Add configurable program parsing to dim_student
- Add combined program services table
## Under the hood
- Add minimum dbt version requirement
- Switch to ranged version pin for edu_edfi_source

# edu_wh v0.2.0
## New features
- Add optional domain disabling to all non-core models, using vars in dbt_project.yml.
- Remove student_school_associations with invalid dates, such as withdraw dates before
    the first day of school, or before the entry date
- Add audit table to capture all invalid school enrollments
- Rework daily attendance model
    - Allow for fractional attendance by changing indicators from bools to floats
    - Roll attendance rates for exited students forward to the end of the year
        so that chronic absenteeism metrics can include exited students in
        daily calculations
    - **Required Migration:** Convert T/F values in `xwalk_attendance_events` to 1.0/0.0

## Under the hood
- Add a single model properties file under each subdirectory, as per DBT recommendation.


# edu_wh v0.1.4
## Under the hood
- Change absenteeism metric boundaries to better match common business rules
## Fixes
- Improve whitespace handling and macro robustness for empty crosswalks

# edu_wh v0.1.3
## New features
- Warehouse models for parents and student-parent linkage
## Fixes
- Bump edu_edfi_source version


# edu_wh v0.1.2
## Under the hood
- Bump edu_edfi_source version


# edu_wh v0.1.1
## New features
- Added parsing for `studentEducationOrganizationAssociation.indicators` in `dim_student`
    - Allows arbitrary student indicators to be mapped into dim_student columns via a xwalk in the project template
- Fact and dimension tables for assessments
    - Preserves all score results and performance levels, while allowing a customizable set to be pulled out as columns
- Added student ids to dim_student
- Fixed typo regarding Chronic Absenteeism buckets
- Add program services
## Under the hood
- Changed chronic absenteeism threshold to be inclusive to better align with common standards
- Added more columns to attendance tables from source data
- Improved handling of extensible column-sets, such that all are optional
    - Added macro `accordion_columns` to help with these cases
## Fixes
- Fixed chronic absenteeism labeling issue

# edu_wh v0.1.0
Initial release
