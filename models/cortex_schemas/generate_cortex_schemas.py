"""
Translate OSI-format seed data into Snowflake Cortex Analyst YAML.

Reads from:
  - osi_semantic_models       (seed)  → model-level metadata & ai_context
  - _osi_resolved_datasets    (SQL)   → datasets with resolved database/schema & ai_context
  - osi_fields                (seed)  → field definitions with ai_context + cortex extensions
  - osi_relationships         (seed)  → join definitions with cortex extensions
  - osi_verified_queries      (seed)  → verified SQL examples
  - osi_metrics               (seed)  → metric definitions (future use)

Outputs one row per semantic model with the translated Cortex YAML.

Run: dbt run --select generate_cortex_schemas
"""
import pandas as pd
import yaml


def model(dbt, session):
    # --- read all OSI seeds + resolved locations ---
    models_df = dbt.ref("osi_semantic_models").to_pandas()
    datasets_df = dbt.ref("_osi_resolved_datasets").to_pandas()
    fields_df = dbt.ref("osi_fields").to_pandas()
    rels_df = dbt.ref("osi_relationships").to_pandas()
    queries_df = dbt.ref("osi_verified_queries").to_pandas()
    metrics_df = dbt.ref("osi_metrics").to_pandas()

    results = []
    for _, sm_row in models_df.iterrows():
        sm_name = sm_row["NAME"]
        cortex = _build_cortex_yaml(
            sm_name=sm_name,
            description=sm_row["DESCRIPTION"],
            ai_context_instructions=sm_row["AI_CONTEXT_INSTRUCTIONS"],
            ai_context_synonyms=sm_row.get("AI_CONTEXT_SYNONYMS"),
            ai_context_examples=sm_row.get("AI_CONTEXT_EXAMPLES"),
            datasets_df=datasets_df[datasets_df["SEMANTIC_MODEL"] == sm_name],
            fields_df=fields_df[fields_df["SEMANTIC_MODEL"] == sm_name],
            rels_df=rels_df[rels_df["SEMANTIC_MODEL"] == sm_name],
            queries_df=queries_df[queries_df["SEMANTIC_MODEL"] == sm_name],
            metrics_df=metrics_df[metrics_df["SEMANTIC_MODEL"] == sm_name],
        )
        yaml_content = yaml.dump(
            cortex,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
        results.append({
            "schema_name": sm_name,
            "yaml_content": yaml_content,
            "generated_at": pd.Timestamp.now(),
        })

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Translation: OSI seeds → Cortex YAML dict
# ---------------------------------------------------------------------------

def _split(val, sep="|"):
    """Split a pipe-delimited string into a list, filtering blanks."""
    if not val or (isinstance(val, float) and pd.isna(val)):
        return []
    return [v.strip() for v in str(val).split(sep) if v.strip()]


def _notnull(val):
    """Return True if val is non-null and non-empty."""
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    return str(val).strip() != ""


def _build_cortex_yaml(sm_name, description, ai_context_instructions,
                        ai_context_synonyms, ai_context_examples,
                        datasets_df, fields_df, rels_df, queries_df,
                        metrics_df):
    cortex = {
        "name": sm_name,
        "description": str(description).strip(),
        "tables": [],
        "relationships": [],
        "verified_queries": [],
    }

    # Model-level custom instructions (Cortex-specific)
    if _notnull(ai_context_instructions):
        cortex["module_custom_instructions"] = {
            "sql_generation": str(ai_context_instructions).strip()
        }

    # --- tables (from OSI datasets) ---
    for _, ds in datasets_df.iterrows():
        dataset_name = ds["DATASET_NAME"]
        table = {
            "name": dataset_name,
            "base_table": {
                "database": ds["TABLE_DATABASE"],
                "schema": ds["TABLE_SCHEMA"],
                "table": str(ds["SOURCE_MODEL"]).upper(),
            },
        }
        if _notnull(ds.get("DESCRIPTION")):
            table["description"] = str(ds["DESCRIPTION"]).strip()

        # Dataset-level ai_context → Cortex synonyms
        syns = _split(ds.get("AI_CONTEXT_SYNONYMS"))
        if syns:
            table["synonyms"] = syns

        pk = _split(ds.get("PRIMARY_KEY"))
        if pk:
            table["primary_key"] = {"columns": pk}

        # Group fields into Cortex buckets
        ds_fields = fields_df[fields_df["DATASET"] == dataset_name]
        dims, time_dims, facts = [], [], []
        for _, f in ds_fields.iterrows():
            col = {
                "name": f["NAME"],
                "expr": f.get("EXPRESSION") or f["NAME"],
                "data_type": str(f.get("CORTEX_DATA_TYPE") or "VARCHAR"),
            }
            if _notnull(f.get("DESCRIPTION")):
                col["description"] = str(f["DESCRIPTION"]).strip()

            # Field-level ai_context → Cortex synonyms
            f_syns = _split(f.get("AI_CONTEXT_SYNONYMS"))
            if f_syns:
                col["synonyms"] = f_syns

            f_samples = _split(f.get("CORTEX_SAMPLE_VALUES"))
            if f_samples:
                col["sample_values"] = f_samples

            # Use OSI is_time_dimension flag first, fall back to cortex_kind
            is_time = str(f.get("IS_TIME_DIMENSION", "false")).lower() == "true"
            cortex_kind = str(f.get("CORTEX_KIND", "dimension")).lower()

            if is_time:
                time_dims.append(col)
            elif cortex_kind == "fact":
                col["access_modifier"] = "public_access"
                facts.append(col)
            else:
                dims.append(col)

        if dims:
            table["dimensions"] = dims
        if time_dims:
            table["time_dimensions"] = time_dims
        if facts:
            table["facts"] = facts

        cortex["tables"].append(table)

    # --- relationships ---
    for _, r in rels_df.iterrows():
        from_cols = _split(r.get("FROM_COLUMNS"))
        to_cols = _split(r.get("TO_COLUMNS"))
        rel = {
            "name": r["NAME"],
            "left_table": r["FROM_DATASET"],
            "right_table": r["TO_DATASET"],
            "relationship_columns": [
                {"left_column": fc, "right_column": tc}
                for fc, tc in zip(from_cols, to_cols)
            ],
        }
        if _notnull(r.get("CORTEX_RELATIONSHIP_TYPE")):
            rel["relationship_type"] = str(r["CORTEX_RELATIONSHIP_TYPE"]).strip()
        if _notnull(r.get("CORTEX_JOIN_TYPE")):
            rel["join_type"] = str(r["CORTEX_JOIN_TYPE"]).strip()
        cortex["relationships"].append(rel)

    # --- verified queries ---
    for _, vq in queries_df.iterrows():
        cortex["verified_queries"].append({
            "name": f'"{vq["QUESTION"]}"',
            "question": str(vq["QUESTION"]).strip(),
            "sql": str(vq["SQL"]).strip(),
            "verified_by": str(vq.get("VERIFIED_BY", "")).strip(),
            "verified_at": _parse_timestamp(vq.get("VERIFIED_AT")),
            "use_as_onboarding_question": str(vq.get("USE_AS_ONBOARDING", "false")).lower() == "true",
        })

    return cortex


def _parse_timestamp(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 0
    s = str(v).strip()
    if not s:
        return 0
    try:
        from datetime import datetime
        return int(datetime.strptime(s[:10], "%Y-%m-%d").timestamp())
    except (ValueError, TypeError):
        return 0
