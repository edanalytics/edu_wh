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
cat > macros/_ci_lint_stub.sql << EOF
{% macro ${dialect}__get_filtered_columns_in_relation(from, except=[]) %}
  {{ return([]) }}
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

# bld_ef3__student_programs and bld_ef3__student_indicators run a live query
# and use its actual results to build columns. bld_ef3__student_assessments_long_results
# asks the warehouse directly for a table's column names. All three need a
# real database connection, so skip them. Tell dbt not to try talking to a
# real database while compiling.
dbt compile --no-introspect --no-populate-cache \
  --select package:edu_wh \
  --exclude bld_ef3__student_programs bld_ef3__student_indicators bld_ef3__student_assessments_long_results \
  --profiles-dir "$profiles_dir" --target dry_run --target-path "$target_path"

# Lint each compiled model on its own so one failure doesn't stop the rest.
fail=0
while IFS= read -r -d '' f; do
  echo "::group::$f"
  if ! sqlfluff lint --config .sqlfluff --templater raw --dialect "$dialect" "$f"; then
    fail=1
  fi
  echo "::endgroup::"
done < <(find "$target_path/compiled" -path "*/edu_wh/models/*" -name "*.sql" -print0)

exit $fail