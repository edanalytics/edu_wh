{% docs school_year_ods_pull %}
School year specified by Spring year, e.g. the 2021-2022 year would be 2022. The school year is populated based on the year associated with the source ODS, which is assumed to contain data for only that school year.
{% enddocs %}

{% docs school_year_source_reference %}
School year specified by Spring year, e.g. the 2021-2022 year would be 2022. Unlike most other tables, `school_year` here reflects a school year reference embedded directly in the source Ed-Fi record, rather than the year of the source ODS pull — so it can differ from the pull year in edge cases (e.g. calendar or schedule data that spans a year boundary).
{% enddocs %}

{% docs school_year_assessment %}
School year specified by Spring year, e.g. the 2021-2022 year would be 2022. For assessment records, `school_year` is primarily populated based on the year associated with the source ODS, which is assumed to contain data for only that school year. If not available from the ODS, it is inferred from `administration_date` using a school-year cutoff date that can vary by implementation (e.g. defaulting to 8/1, meaning an assessment administered on or after 8/1/2021 and before 8/1/2022 is assigned school year 2022). Some implementations may also configure a school year crosswalk to override the ODS-sourced or inferred year for specific assessments or date ranges.
{% enddocs %}

{% docs school_year_academic_record %}
School year specified by Spring year, e.g. the 2021-2022 year would be 2022. Unlike most other tables, `school_year` here is sourced from the `student_academic_records` Ed-Fi endpoint, where it is a required field provided directly by the source system rather than inferred from a single-year ODS.
{% enddocs %}
