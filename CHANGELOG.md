# Unreleased
## New features
## Under the hood
## Fixes 
- Implement "first day school" rule for single-calendar schools in fct_student_school_association
- Allow extension columns to correctly be pulled into the stacked fct_program_service table

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
