# Cross-Tenant Assessment Feature

## For Users

### What this feature does

When this feature is enabled, the warehouse surfaces a student's complete assessment history to every tenant where that student has an active enrollment — regardless of which tenant originally collected the data. This gives the **active tenant** a full view of student performance across all organizations, not just their own.

Two terms used throughout this doc:
- **Active tenant** — the tenant where the student currently has an active enrollment
- **Source tenant** — the tenant that originally administered and collected the assessment

In practice, this means assessment records are duplicated across tenants. The surrogate keys (e.g., `k_assessment`) that make up the grain of the affected tables are scoped to the **active tenant**, not the source tenant. Therefore, this is not a breaking change to the grain of any tables. But your downstream metrics/dashboards will surface more results to users, so it's important to get partner approval before enabling.

See **For Developers** below for how to enable this feature.

```
Two years ago                              Today
────────────────────────────────────────────────────────────────
Student enrolled at District A             Student enrolled at District B
District A administers assessments         (active tenant)
→ assessment records created                    │
  (source tenant = District A)                  │
        │                                       ▼
        └───────────────────────────→  District B can now see
                                       District A's assessment records
                                       attributed to District B
```

The same assessment event produces two rows in `fct_student_assessment` — one per tenant. The natural key fields (student, assessment, date, score) are identical; only the surrogate keys and tenant columns differ:

| k_student_assessment | k_assessment | tenant_code | student_unique_id | assessment_identifier | administration_date | scale_score | is_original_record | original_tenant_code |
|---|---|---|---|---|---|---|---|---|
| `a1b2c3...` | `f6g7h8...` | district_a | STU-0042 | SAT | 2023-10-15 | 1200 | `true` | district_a |
| `x9y8z7...` | `p2q3r4...` | district_b | STU-0042 | SAT | 2023-10-15 | 1200 | `false` | district_a |

### When does a record get copied?

A student's assessment records are copied to the active tenant when:
- The student has an active school enrollment at the active tenant
- The active tenant is different from the source tenant
- The student can be matched across tenants by `student_unique_id`

Identity validation is applied to guard against false matches where two different students share a `student_unique_id` across tenants. If validation fails, no copy is created.

**Default validation logic** (used when `edu:assessments:removed_students_source` is not configured):
- The student has an active enrollment at a tenant different from the source tenant
- AND at least one of the following must be true:
  - `student_unique_id` is identical across all tenants
    - NOTE, this feature relies on the assumption that `student_unique_id` is portable across tenants. If each tenant defines their own student identification system, this feature will not work (as currently written).
  - Birthdate is identical across all tenants
  - OR at least one of {birthdate, first name, last name} is consistent across all tenants
    - i.e. the sum of distinct values across all three fields is ≤ 5

**Configurable override** (`edu:assessments:removed_students_source`):
- Provide a seed or model containing `k_student` values to explicitly identify students who should be excluded from cross-tenant matching
- When this is set, the default logic above is bypassed entirely — only students in the source are excluded

### Which tables are affected?

| Table | What's added |
|---|---|
| `fct_student_assessment` | One additional row per assessment per additional tenant the student is enrolled in |
| `fct_student_objective_assessment` | Same, for each objective/sub-assessment result |
| `dim_assessment` | One additional row per assessment per tenant where a cross-tenant student has taken it |
| `dim_objective_assessment` | Same, for each objective assessment |

### How to tell original from copied records

Every row in `fct_student_assessment` and `fct_student_objective_assessment` carries two columns:

- `is_original_record` — `true` if `tenant_code` is the source tenant (the one that administered the assessment); `false` if it is an active-tenant copy
- `original_tenant_code` — the source tenant that originally collected the data

The score values, dates, and all other assessment data are identical between the original and its copies.

### The main thing to watch out for: double-counting

If you query `fct_student_assessment` across multiple tenants without filtering, a student enrolled in two tenants will appear twice for the same assessment — once per tenant. Any count or aggregation that should reflect unique assessment events needs a filter:

```sql
-- count unique assessment events, not tenant copies
where is_original_record = true
```

If you're already scoping a query to a single tenant, this is not an issue — you'll see original records for students whose data lives in that tenant, plus copies for students whose data lives elsewhere.

### Joining dim tables

`dim_assessment` and `dim_objective_assessment` also contain one row per tenant. When joining from a fact table, the surrogate keys (`k_assessment`, `k_objective_assessment`) are already tenant-scoped, so joins work correctly without any additional filtering.

---

## For Developers

### Feature flag and entry point

The feature is controlled by the variable `edu:assessment:cross_tenant_enabled` (default `false`). When `false`, `bld_ef3__student_assessment_cross_tenant` returns zero rows and all downstream cross-tenant logic is a no-op.

### The central models: `bld_ef3__student_assessment_cross_tenant` and `bld_ef3__student_objective_assessment_cross_tenant`

Cross-tenant logic is anchored by two build models that serve as the single source of truth for remapped surrogate keys.

**`bld_ef3__student_assessment_cross_tenant`** produces one row per (student assessment × tenant enrollment), in two categories:

- **Original rows** (`is_original_record = true`): the student assessment as it exists in its source tenant, mirroring source data 1:1.
- **Cross-tenant rows** (`is_original_record = false`): copies attributed to a different tenant, with surrogate keys rebuilt using the new `tenant_code`. The assessment data itself is unchanged; only the keys change.

The model exposes `k_student_assessment__original` and `k_assessment__original` — the pre-remap keys — which downstream models use to join back to staging data.

**`bld_ef3__student_objective_assessment_cross_tenant`** extends that logic to the objective-assessment level. It joins `stg_ef3__student_objective_assessments` to `bld_ef3__student_assessment_cross_tenant` via `k_student_assessment__original`, then rebuilds `k_student_objective_assessment` and `k_objective_assessment` surrogate keys for each target tenant. A final dedup on `k_student_objective_assessment` (preferring `is_original_record = true`) collapses any duplicates that arise when the same assessment result appears in multiple tenants. The model exposes `k_student_objective_assessment__original` and `k_objective_assessment__original` for downstream joins to staging data.

### Record flow

**Assessment-level**

| Model | Input | Join | Output |
|---|---|---|---|
| `bld_ef3__student_assessment_cross_tenant` | `stg_ef3__student_assessments` × `fct_student_school_association` | — (produces the mapping) | 1 row per (assessment × enrolled tenant); original + cross-tenant rows; deduped on `k_student_assessment` |
| `fct_student_assessment` | `stg_ef3__student_assessments` | left join build model via `k_student_assessment__original`; coalesce keys | One row per assessment per tenant; students with no cross-tenant enrollment pass through unchanged |
| `dim_assessment` | `stg_ef3__assessments` | left join build model via `k_assessment__original`; coalesce keys | One row per assessment per tenant |

**Objective-assessment-level**

| Model | Input | Join | Output |
|---|---|---|---|
| `bld_ef3__student_objective_assessment_cross_tenant` | `stg_ef3__student_objective_assessments` | join `bld_ef3__student_assessment_cross_tenant` via `k_student_assessment__original` | 1 row per (objective assessment × enrolled tenant); deduped on `k_student_objective_assessment` |
| `fct_student_objective_assessment` | `stg_ef3__student_objective_assessments` | left join build model via `k_student_objective_assessment__original`; coalesce keys | Final dedup on `k_student_objective_assessment` after wide select |
| `dim_objective_assessment` | `stg_ef3__objective_assessments` | left join build model (pre-deduped to `k_objective_assessment`) via `k_objective_assessment__original`; coalesce keys | Final dedup on `(k_assessment, k_objective_assessment)` |

### Edge cases

| Scenario | Where handled | Outcome |
|---|---|---|
| Student fails identity validation (default mode) | `bld_ef3__student_assessment_cross_tenant` `qualify` block | No cross-tenant rows created for that student |
| Student in `removed_students_source` | `bld_ef3__student_assessment_cross_tenant` left join + filter | No cross-tenant rows created for that student |
| Prior to this feature, two tenants independently ingested the same assessment record (e.g. both loaded the same state file)| `bld_ef3__student_assessment_cross_tenant` dedup (prefer `is_original_record`) | One row per `k_student_assessment` |
| Same as above, at the objective-assessment level | `bld_ef3__student_objective_assessment_cross_tenant` dedup + `fct_student_objective_assessment` final dedup | One row per `k_student_objective_assessment`; target tenant's native record wins |
| Student no longer enrolled at source tenant | Separate UNION branch in `bld_ef3__student_assessment_cross_tenant` preserves original record | Original record always retained |
| Historic assessment with no matching `dim_student` row | Left join to `dim_student` in fact tables | Record preserved with null `k_student` |
| Student enrolled in N active tenants | Cross-tenant build model fans out | N rows total (1 original + N−1 copies) |

### Testing

- Enable the feature flag in your dev environment (`edu:assessment:cross_tenant_enabled: true`)
- Confirm with the partner that student matching and exclusion rules are being applied correctly — verify that the right students are being matched across tenants and that any `removed_students_source` exclusions are working as intended
- Confirm dbt tests are not failing — in particular, the FK `relationships` tests on `fct_student_objective_assessment.k_objective_assessment → dim_objective_assessment.k_objective_assessment` and the PK constraints on all affected fact/dim tables
- Run QC queries to validate record flow end to end — e.g. confirm that `student_unique_id` is consistent between original and cross-tenant copies, that `is_original_record` / `original_tenant_code` are populated correctly, and that aggregations scoped to `is_original_record = true` match pre-feature baselines
