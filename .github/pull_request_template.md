<!---
Provide Title above, ensure it summarizes the work in the PR. Example PR titles templates:
* "feature/ (or build/): describe new functionality"
* "hotfix/: describe issue fix for immediate release"
* "bugfix/ (or fix/): describe issue fix, not necessary for immediate release"
* "docs/: adding or updating documentation"
-->

## Description & motivation
<!---
High level description your PR, and why you're making it. Is this linked to slack thread, Monday board, open
issue, a continuation to a previous PR? Link it here if relevant (use the "#" symbol for issues/PRs).
-->

## Breaking changes introduced by this PR:
<!---
Describe any breaking changes that are introduced. Take a wide approach to what might be 'breaking'. One example of a 'hidden' breaking change could be adding a new column. For most applications, this will not cause error, but if someone has configured a downstream query that references this column name from elsewhere, their query could break.

Make sure to include these breaking changes in the CHANGELOG.md
-->

## PR Merge Priority:
<!---
This checklist helps the reviewers understand the level of priority for merging this PR.
A loose description of merging priority levels is:
Low: A week or more.
Medium: Within 3 days or less.
High: As soon as possible.
-->
- [ ] Low
- [ ] Medium
- [ ] High

<!---
If High Priority, explain why as a comment below.
-->

## Changes to existing files:
<!---
Include this section if you are changing any existing files or creating breaking changes to existing files. Label the model name and describe the logic behind the changes made, try to be very descriptive here. For example:
- `stg_model` : Describe any changes made to `stg_model` and why the changes where made.
- `src_staging` : Describe any changes made to `src_staging` and why the changes where made.
-->

## New files created:
<!---
Include this section if you are creating any new files. Label the model name and describe the logic behind the changes made, try to be very descriptive here. For example:
- `stg_new_model` : Describe the purpose of `stg_new_model` and the logic behind creating the model.
- `src_new_staging` : Describe the purpose of `src_new_staging`.
-->

## Tests and QC done:
<!---
Describe any process that confirms that the files do what is expected, include screenshots if relevant. For example:
- Analyst replication confirmed that updates to `stg_model` new counts were correct.
- Executed a dbt project run and ensured it was successful.
-->

## edu_wh PR Review Checklist:
Make sure the following have been completed before approving this PR:
- [ ] Description of changes has been added to Unreleased section of [CHANGELOG.md](/CHANGELOG.md). Add under `## New Features` for features, etc.
- [ ] If a new configuration xwalk was added:
  - [ ] The code is written such that the xwalk is optional (preferred), and this behavior was tested, OR
  - [ ] The code is written such that the xwalk is required, and the required xwalk is added to edu_project_template, and this PR is flagged as breaking change (not for patch release)
  - [ ] A description for the new xwalk has been added to EDU documentation site [here](https://github.com/edanalytics/edu_docs/blob/main/docs/docs/manage_extend/reference/configure_dbt_xwalks.md) 
- [ ] If a new configuration variable was added:
  - [ ] The code is written such that the variable is optional (preferred), and this behavior was tested, OR
  - [ ] The code is written such that the variable is required, and a default value was added to edu_project_template, and this PR is flagged as breaking change (not for patch release)
  - [ ] A description for the new variable has been added to EDU documentation site [here](https://github.com/edanalytics/edu_docs/blob/main/docs/docs/manage_extend/reference/configure_dbt_vars.md) 
- [ ] Reviewer confirms the grain of all tables are unchanged, OR any changes are expected, communicated, and this PR is flagged as a breaking change (not for patch release)

<!---## Future ToDos & Questions:-->
<!---
[Optional] Include any future steps and questions related to this PR.
-->
