# Seeds

## osi/ (OSI-format semantic model seeds)

Defines semantic models in a normalized, OSI-aligned format. `osi_semantic_models`
is the **top-level parent** -- all other seeds reference it via `semantic_model`.
`ai_context` (instructions, synonyms, examples) is available at every level.

| Seed | Grain | ai_context columns |
|------|-------|--------------------|
| `osi_semantic_models` | One row per semantic model | `ai_context_instructions`, `ai_context_synonyms`, `ai_context_examples` |
| `osi_datasets` | One row per dataset (table) per model | `ai_context_synonyms`, `ai_context_instructions` |
| `osi_fields` | One row per field per dataset | `ai_context_synonyms` |
| `osi_relationships` | One row per join per model | `ai_context_synonyms` |
| `osi_metrics` | One row per metric per model | `ai_context_synonyms` |
| `osi_verified_queries` | One row per verified SQL example per model | (n/a) |

### Cortex extensions

Some columns are prefixed with `cortex_` to indicate Snowflake Cortex Analyst-specific
metadata that is not part of the core OSI spec:

- `osi_fields`: `cortex_kind`, `cortex_data_type`, `cortex_sample_values`
- `osi_relationships`: `cortex_relationship_type`, `cortex_join_type`

### How it flows

1. **Seeds** define the semantic model in OSI format
2. **`_osi_resolved_datasets`** (ephemeral SQL) joins `osi_datasets` with `_cortex_table_locations` to resolve physical database/schema
3. **`generate_cortex_schemas`** (Python model) reads all seeds, translates to Cortex Analyst YAML
4. Post-hooks upload YAML to stage and create the semantic view

### Regenerate from ai_schemas/

If you still maintain the old YAML format:

```bash
cd /path/to/edu_wh
python3 scripts/convert_ai_schema_to_osi_seeds.py
```

Then run `dbt seed && dbt run -s generate_cortex_schemas` (from the root project).
