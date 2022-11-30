
# Unreleased
## New features
- Added parsing for `studentEducationOrganizationAssociation.indicators` in `dim_student`
    - Allows arbitrary student indicators to be mapped into dim_student columns via a xwalk in the project template

## Under the hood
- Changed chronic absenteeism threshold to be inclusive to better align with common standards
- Added more columns to attendance tables from source data
- Improved handling of extensible `dim_student` columns such that neither `characteristics` nor `indicators` are required

## Fixes
- Fixed chronic absenteeism labeling issue

# edu_wh v0.1.0
Initial release