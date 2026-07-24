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
needs_real_warehouse=()
compile_log=$(mktemp)
trap 'rm -f "$compile_log"' EXIT

while true; do
  exclude_flags=()
  [[ ${#needs_real_warehouse[@]} -gt 0 ]] && exclude_flags=(--exclude "${needs_real_warehouse[@]}")

  dbt compile --no-introspect --no-populate-cache \
    --select package:edu_wh \
    "${exclude_flags[@]}" \
    --vars '{"edu:tpdm:enabled": true, "src:domain:tpdm:enabled": true, "src:domain:tpdmcommunity:enabled": true, "extensions": {
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
  culprit=""
  grep -q "connection never acquired for thread" "$compile_log" \
    && culprit=$(grep -oE "Runtime Error in (model|test) [a-zA-Z0-9_]+" "$compile_log" | head -1 | awk '{print $NF}')

  if [[ -z "$culprit" ]] || [[ " ${needs_real_warehouse[*]} " == *" $culprit "* ]]; then
    echo "❌ dbt compile failed:"
    cat "$compile_log"
    exit 1
  fi

  echo "⚠️ $culprit needs a live warehouse connection to compile, adding $culprit to the exclude list and recompiling"
  needs_real_warehouse+=("$culprit")
done

# Lint each compiled model on its own so one failure doesn't stop the rest.
# Passing models print one line; failing ones get a collapsible group with
# the full sqlfluff output inside.
mapfile -d '' -t compiled_files < <(find "$target_path/compiled" -path "*/edu_wh/models/*" -name "*.sql" -print0)
total=${#compiled_files[@]}
pass=0
fail=0
failed=()
for f in "${compiled_files[@]}"; do
  name=$(basename "$f")
  path=${f#*/edu_wh/}
  if out=$(sqlfluff lint --config .sqlfluff --templater raw --dialect "$dialect" "$f" 2>&1); then
    pass=$((pass + 1))
    echo "Linting $path ✅"
  else
    fail=$((fail + 1))
    failed+=("${name%.sql}")
    printf '::group::Linting %s ❌\n%s\n::endgroup::\n' "$path" "$out"
  fi
done

echo ""
printf '✅ %d/%d models compatible with %s\n' "$pass" "$total" "$dialect"
printf '⚠️ %d models cannot be compiled here and requires live warehouse, lint these models locally instead:\n' "${#needs_real_warehouse[@]}"
printf '    - %s\n' "${needs_real_warehouse[@]}"
if [[ ${#failed[@]} -eq 0 ]]; then
  exit 0
else
  printf '❌ %d models are NOT compatible with %s:\n' "${#failed[@]}" "$dialect"
  printf '    - %s\n' "${failed[@]}"
  exit 1
fi