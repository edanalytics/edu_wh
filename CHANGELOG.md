# Unreleased
## New features
- Add new table `fct_student_contact_information`, which includes student addresses, email addresses, phone, and languages. This gives more detailed contact info than `dim_student`, and can be given different security rules than `dim_student`.
  - Note: was originally planned for v0.3.2, but we need to think more about the correct structure for this table.
## Under the hood
## Fixes 

# edu_wh v0.3.2
## New features
- Add `has_hispanic_latino_ethnicity` to `dim_student`. Also include in this in fields that are "immutable" (consistent across years), assuming variable `edu:stu_demos:make_demos_immutable` is set to `True`.
- Add configurable student language columns to `dim_student`.
- Add configurable custom override for student grade level (some source other than student-school-assoc in Ed-Fi). Use variable `edu:stu_demos:grade_level_override` to configure a data source and column.
- Add configurable logic for override of NULL `school_year` in `fct_student_assessment` and `fct_student_objective_assessment`. By default, compare administration_dates to 8/1 to determine year, but allow for override of threshold or an optional xwalk by year.
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
