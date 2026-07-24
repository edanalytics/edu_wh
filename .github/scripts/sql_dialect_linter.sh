#!/usr/bin/env bash

# Usage: lint_dbt.sh <dialect>   where dialect is `databricks` or `snowflake`.

set -euo pipefail

# Which warehouse are we pretending to be: databricks or snowflake?
dialect="${1:-}"
case "$dialect" in
  databricks|snowflake) ;;
  *) echo "usage: $0 <databricks|snowflake>" >&2; exit 2 ;;
esac

# Read the project's name so we can build a fake connection for it below.
profile=$(grep "^profile:" dbt_project.yml | awk '{print $2}' | tr -d "'\"")
# Keep each dialect's compiled output in its own folder so they don't overwrite each other.
target_path="target/lint-${dialect}"
profiles_dir="${target_path}/dummy_profile"
mkdir -p "$profiles_dir"

# per-dialect dummy profile.
if [[ "$dialect" == "databricks" ]]; then
  cat > "$profiles_dir/profiles.yml" << EOF
$profile:
  target: dry_run
  outputs:
    dry_run:
      type: databricks
      host: 127.0.0.1
      http_path: /sql/1.0/warehouses/dummy
      token: dummy
      schema: dummy
      catalog: dummy
      threads: 8
      connect_timeout: 1
      connect_retries: 0
      retry_all: false
      connection_parameters:
        _retry_stop_after_attempts_count: 1
        _socket_timeout: 3
EOF
else
  cat > "$profiles_dir/profiles.yml" << EOF
$profile:
  target: dry_run
  outputs:
    dry_run:
      type: snowflake
      account: dummy-account
      user: dummy
      password: dummy
      role: dummy
      database: dummy
      warehouse: dummy
      schema: dummy
      threads: 8
      connect_retries: 0
      connect_timeout: 3
      retry_all: false
EOF
fi

# Each of these normally hits the real warehouse during compile, so replace
# them with fake versions that just return an empty result:
#   get_filtered_columns_in_relation / unpivot -> list a real table's columns
#   get_column_values                          -> select distinct values
#   get_intervals_between                      -> a real date-diff query
#   is_incremental                             -> checks if the table exists
#   union_relations                            -> also lists a table's columns
cat > macros/_ci_lint_stub.sql << EOF
{% macro ${dialect}__get_filtered_columns_in_relation(from, except=[]) %}
  {{ return([]) }}
{% endmacro %}

{% macro ${dialect}__union_relations(relations, column_override=none, include=[], exclude=[], source_column_name='_dbt_source_relation', where=none) %}
  {{ return('') }}
{% endmacro %}

{% macro ${dialect}__get_column_values(table, column, order_by='count(*) desc', max_records=none, default=none, where=none) %}
  {{ return(default if default is not none else []) }}
{% endmacro %}

{% macro is_incremental() %}
  {{ return(False) }}
{% endmacro %}

{% macro ${dialect}__get_intervals_between(start_date, end_date, datepart) %}
  {{ return(1) }}
{% endmacro %}

{% macro ${dialect}__unpivot(relation=none, cast_to='varchar', exclude=none, remove=none, field_name='field_name', value_name='value', quote_identifiers=False) %}
  select
    {% for col in (exclude or []) %}
    cast(null as {{ cast_to }}) as {{ col }},
    {% endfor %}
    cast(null as {{ cast_to }}) as {{ field_name }},
    cast(null as {{ cast_to }}) as {{ value_name }}
  where false
{% endmacro %}

{#- edu_edfi_source's own databricks__json_flatten emits "lateral variant_explode(...)",
which is real, working Databricks SQL (Databricks' variant type + lateral table
functions), but sqlfluff's databricks dialect can't parse it yet. Reproduce the
exact same SQL and just tell sqlfluff to skip the parse check on that line -#}
{% macro databricks__json_flatten(column, alias, outer) -%}
, lateral variant_explode{% if outer %}_outer{% endif %}({{ column }}) {% if alias != '' %} as {{ alias }} {% endif %} -- noqa: PRS
{%- endmacro %}
EOF


# get_single_value (this repo) and extract_descriptor (edu_edfi_source) also
# run real queries against the warehouse, but can't be faked the same way as
# above. So we replace their whole file with a simple version that skips the
# query and just returns a default answer.
cat > macros/get_single_value.sql << 'EOF'
{% macro get_single_value(query, default) %}
  {{ return(default) }}
{% endmacro %}
EOF

cat > dbt_packages/edu_edfi_source/macros/extract_descriptor.sql << 'EOF'
{% macro extract_descriptor(col, descriptor_name=None) -%}
  split_part({{ col }}, '#', -1)
{%- endmacro %}
EOF

# Generate a placeholder covering every resource name actually referenced, 
# with a fake database/schema, so source() resolves without needing a real one.
resources=$(grep -rhoE "source_edfi3\(\s*['\"][a-zA-Z0-9_]+['\"]" \
    dbt_packages/edu_edfi_source/models dbt_packages/edu_edfi_source/macros 2>/dev/null \
    | sed -E "s/source_edfi3\(\s*['\"]//; s/['\"]//" | sort -u)

{
  echo "sources:"
  echo "  - name: raw_edfi_3"
  echo "    database: dummy"
  echo "    schema: dummy_raw"
  echo "    tables:"
  echo "      - name: _deletes"
  for r in $resources; do
    echo "      - name: $r"
  done
} > dbt_packages/edu_edfi_source/models/_ci_lint_sources.yml

# Generate an empty placeholder seed for every ref() that isn't defined
# anywhere else, so ref() resolves without needing the real crosswalk data.
mkdir -p seeds
{
  find . dbt_packages/edu_edfi_source -iname "*.sql" -path "*/models/*" 2>/dev/null | sed -E 's#.*/##; s/\.sql$//'
  find . dbt_packages/edu_edfi_source -iname "*.csv" -path "*/seeds/*" 2>/dev/null | sed -E 's#.*/##; s/\.csv$//'
} | sort -u > /tmp/_ci_defined_nodes.txt

grep -rhoE "ref\(['\"][a-zA-Z0-9_]+['\"]" . dbt_packages/edu_edfi_source --include="*.sql" 2>/dev/null \
  | sed -E "s/ref\(['\"]//; s/['\"]//" | sort -u > /tmp/_ci_all_refs.txt

for missing in $(comm -23 /tmp/_ci_all_refs.txt /tmp/_ci_defined_nodes.txt); do
  printf 'placeholder\nx\n' > "seeds/${missing}.csv"
done

# Some models talk to the warehouse directly for real data or column info,
# so they can't compile against a fake connection. Any OTHER kind of failure (a real bug, a missing
# config) stops the script.
#
# fct_student_program_service also needs an "extensions" answer for each of
# these 7 program names, or it errors out asking for a setting that's
# normally supplied by implementation.
#
# tpdm_warehouse and finance_warehouse (and their edu_edfi_source staging
# models) are disabled by default. turn them on so they get linted too.
models_needing_warehouse=()
tests_needing_warehouse=()
compile_log=$(mktemp)
trap 'rm -f "$compile_log"' EXIT

while true; do
  exclude_flags=()
  all_excluded=("${models_needing_warehouse[@]}" "${tests_needing_warehouse[@]}")
  [[ ${#all_excluded[@]} -gt 0 ]] && exclude_flags=(--exclude "${all_excluded[@]}")

  dbt compile --no-introspect --no-populate-cache \
    --select package:edu_wh \
    "${exclude_flags[@]}" \
    --vars '{"edu:tpdm:enabled": true, "src:domain:tpdm:enabled": true, "src:domain:tpdmcommunity:enabled": true, "edu:finance:enabled": true, "src:domain:finance:enabled": true, "extensions": {
      "stg_ef3__stu_spec_ed__program_services": {},
      "stg_ef3__stu_lang_instr__program_services": {},
      "stg_ef3__stu_homeless__program_services": {},
      "stg_ef3__stu_title_i_part_a__program_services": {},
      "stg_ef3__stu_cte__program_services": {},
      "stg_ef3__stu_migrant_edu__program_services": {},
      "stg_ef3__stu_school_food_service__program_services": {}
    }}' \
    --profiles-dir "$profiles_dir" --target dry_run --target-path "$target_path" \
    > "$compile_log" 2>&1 && break

  # dbt's error looks like "Runtime Error in model some_model_name (path/to/file.sql)"
  # or "...in test some_test_name (...)". Pull out both the node type and its name.
  culprit=""
  culprit_type=""
  grep -q "connection never acquired for thread" "$compile_log" \
    && culprit_type=$(grep -oE "Runtime Error in (model|test)" "$compile_log" | head -1 | awk '{print $NF}') \
    && culprit=$(grep -oE "Runtime Error in (model|test) [a-zA-Z0-9_]+" "$compile_log" | head -1 | awk '{print $NF}')

  if [[ -z "$culprit" ]] || [[ " ${all_excluded[*]} " == *" $culprit "* ]]; then
    echo "❌ dbt compile failed:"
    cat "$compile_log"
    exit 1
  fi

  echo "⚠️ $culprit needs a live warehouse connection to compile, adding $culprit to the exclude list and recompiling"
  if [[ "$culprit_type" == "test" ]]; then
    tests_needing_warehouse+=("$culprit")
  else
    models_needing_warehouse+=("$culprit")
  fi
done

# Lint each compiled model/test on its own so one failure doesn't stop the
# rest. Passing ones print one line; failing ones get a collapsible group
# with the full sqlfluff output inside. Generic tests (auto-generated from
# schema .yml files, like unique/not_null) compile into a "<node>.yml/"
# subfolder — split those out from real models so the counts mean what they say.
mapfile -d '' -t compiled_files < <(find "$target_path/compiled" -path "*/edu_wh/models/*" -name "*.sql" -print0)
model_total=0; model_pass=0; model_fail=0; model_failed=()
test_total=0; test_pass=0; test_fail=0; test_failed=()
for f in "${compiled_files[@]}"; do
  name=$(basename "$f")
  path=${f#*/edu_wh/}
  is_test=false
  [[ "$path" == *.yml/* ]] && is_test=true

  if out=$(sqlfluff lint --config .sqlfluff --templater raw --dialect "$dialect" "$f" 2>&1); then
    echo "Linting $path ✅"
    if $is_test; then test_pass=$((test_pass + 1)); else model_pass=$((model_pass + 1)); fi
  else
    printf '::group::Linting %s ❌\n%s\n::endgroup::\n' "$path" "$out"
    if $is_test; then
      test_fail=$((test_fail + 1)); test_failed+=("${name%.sql}")
    else
      model_fail=$((model_fail + 1)); model_failed+=("${name%.sql}")
    fi
  fi
  if $is_test; then test_total=$((test_total + 1)); else model_total=$((model_total + 1)); fi
done

echo ""
model_known=$((model_total + ${#models_needing_warehouse[@]}))
printf '✅ %d/%d models compatible with %s\n' "$model_pass" "$model_known" "$dialect"
if [[ ${#models_needing_warehouse[@]} -gt 0 ]]; then
  printf '⚠️ %d of those %d cannot be compiled here and requires live warehouse, lint these models locally instead:\n' "${#models_needing_warehouse[@]}" "$model_known"
  printf '    - %s\n' "${models_needing_warehouse[@]}"
fi

echo ""
test_known=$((test_total + ${#tests_needing_warehouse[@]}))
printf '✅ %d/%d tests compatible with %s\n' "$test_pass" "$test_known" "$dialect"
if [[ ${#tests_needing_warehouse[@]} -gt 0 ]]; then
  printf '⚠️ %d of those %d cannot be compiled here and requires live warehouse, lint these tests locally instead:\n' "${#tests_needing_warehouse[@]}" "$test_known"
  printf '    - %s\n' "${tests_needing_warehouse[@]}"
fi

if [[ ${#model_failed[@]} -eq 0 && ${#test_failed[@]} -eq 0 ]]; then
  exit 0
fi
echo ""
if [[ ${#model_failed[@]} -gt 0 ]]; then
  printf '❌ %d models are NOT compatible with %s:\n' "${#model_failed[@]}" "$dialect"
  printf '    - %s\n' "${model_failed[@]}"
fi
if [[ ${#test_failed[@]} -gt 0 ]]; then
  printf '❌ %d tests are NOT compatible with %s:\n' "${#test_failed[@]}" "$dialect"
  printf '    - %s\n' "${test_failed[@]}"
fi
exit 1