# Unreleased
## New features
## Under the hood
## Fixes 


# edu_wh v0.2.9
## New features
- Add ability to extend `dim_course` and `dim_class_period` with external data sources
- Add an `is_latest_record` indicator to `dim_student` to identify the demographics from the most recent school year in which a student appeared
## Under the hood
- In `dim_student`, choose the grade level from the most recent school enrollment, rather than the longest duration. This better aligns with grade transition patterns seen in the wild.


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
