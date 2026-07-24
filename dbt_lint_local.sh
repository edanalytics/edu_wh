dbt_lint() {
    # Default settings; override with --dialect or --templater.
    local templater="raw" dialect="snowflake"

    # Any other arguments are model names to lint. With none, lint everything.
    local selectors=() skip_compile=false
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --templater)    templater="$2"; shift 2 ;;
            --dialect)      dialect="$2";   shift 2 ;;
            --skip-compile) skip_compile=true; shift ;;
            --*)            echo "[dbt-lint] unknown option: $1"; return 1 ;;
            *)              selectors+=("$1"); shift ;;
        esac
    done
    local lint_all=false
    [[ ${#selectors[@]} -eq 0 ]] && lint_all=true

    # Look for .sqlfluff next to this script, not wherever it's being run from.
    local config
    config="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.sqlfluff"

    # For any dialect besides snowflake, fake a connection so dbt can
    # compile without needing real warehouse credentials.
    local compile_flags=() search_root="./target/compiled"
    if [[ "$dialect" != "snowflake" ]]; then
        local profile lint_target_path dummy_profiles_dir
        profile=$(grep "^profile:" dbt_project.yml 2>/dev/null | awk '{print $2}' | tr -d "'\"")
        lint_target_path="target/lint-${dialect}"
        dummy_profiles_dir="${lint_target_path}/dummy_profile"
        mkdir -p "$dummy_profiles_dir"
        cat > "$dummy_profiles_dir/profiles.yml" << EOF
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
      # Without this, dbt can hang for a long time trying to reach the fake host.
      connection_parameters:
        _retry_stop_after_attempts_count: 1
        _socket_timeout: 3
EOF
        compile_flags=(--profiles-dir "$dummy_profiles_dir" --target dry_run --target-path "$lint_target_path")
        search_root="./${lint_target_path}/compiled"
    fi

    # Lint just the given models, or the whole project if none were given.
    local select_flags=()
    [[ "$lint_all" == false ]] && select_flags=(--select "${selectors[@]}")

    if [[ "$skip_compile" == true ]]; then
        # Reuse whatever was already compiled last time, for a quick recheck.
        search_root="./target/compiled"
        echo "[dbt-lint] skipping compile, linting existing target/compiled as $dialect"
    else
        if [[ "$lint_all" == true ]]; then
            echo "[dbt-lint] compiling all models → $dialect"
        else
            echo "[dbt-lint] compiling → $dialect: ${selectors[*]}"
        fi

        # A few checks and macros (like is_incremental) try to query the real
        # warehouse while compiling, even though we don't have one. Replace
        # them with fake versions that just return a safe, empty answer.
        local stub_macro_file=""
        if [[ "$dialect" != "snowflake" ]]; then
            stub_macro_file="macros/_dbt_lint_stub.sql"
            trap 'rm -f "$stub_macro_file"; trap - RETURN' RETURN
            cat > "$stub_macro_file" << 'EOF'
{% macro databricks__get_filtered_columns_in_relation(from, except=[]) %}
  {{ return([]) }}
{% endmacro %}

{% macro databricks__get_column_values(table, column, order_by='count(*) desc', max_records=none, default=none, where=none) %}
  {{ return(default if default is not none else []) }}
{% endmacro %}

{% macro databricks__unpivot(relation=none, cast_to='varchar', exclude=none, remove=none, field_name='field_name', value_name='value', quote_identifiers=False) %}
  select
    {% for col in (exclude or []) %}
    cast(null as {{ cast_to }}) as {{ col }},
    {% endfor %}
    cast(null as {{ cast_to }}) as {{ field_name }},
    cast(null as {{ cast_to }}) as {{ value_name }}
  where false
{% endmacro %}

{% macro is_incremental() %}
  {{ return(False) }}
{% endmacro %}

{% macro databricks__get_intervals_between(start_date, end_date, datepart) %}
  {{ return(1) }}
{% endmacro %}
EOF
        fi

        local compile_start=$SECONDS log_file compile_status
        log_file=$(mktemp)
        DATABRICKS_RETRY_MAX=0 dbt compile --no-introspect --no-populate-cache "${select_flags[@]}" "${compile_flags[@]}" 2>&1 \
            | tee "$log_file" | grep -E '^[0-9]{2}:[0-9]{2}:[0-9]{2}'
        compile_status=${PIPESTATUS[0]}
        echo "[dbt-lint] compile finished in $((SECONDS - compile_start))s · log: $log_file"
        if [[ $compile_status -ne 0 ]]; then
            echo "[dbt-lint] compile failed — last log lines:"
            tail -30 "$log_file"
            return 1
        fi
    fi

    # Find every compiled model, or just the ones the caller asked for.
    local paths=() found model
    if [[ "$lint_all" == true ]]; then
        while IFS= read -r found; do paths+=("$found"); done \
            < <(find -L "$search_root" -path "*/models/*" -name "*.sql" 2>/dev/null | sort)
    else
        for model in "${selectors[@]}"; do
            found=$(find -L "$search_root" -name "${model}.sql" 2>/dev/null | head -1)
            [[ -z "$found" ]] && { echo "[dbt-lint] compiled file not found for '$model'"; continue; }
            paths+=("$found")
        done
    fi
    [[ ${#paths[@]} -eq 0 ]] && { echo "[dbt-lint] nothing to lint"; return 1; }

    # Print a pass/fail symbol per model as we go, then show full details
    # for anything that failed once everything's done.
    local total=${#paths[@]} idx=0 pass=0 fail=0 err=0 failed=()
    local details lint_start=$SECONDS path name out status t0 mark
    details=$(mktemp)
    printf '\n[dbt-lint] checking %d model(s) against %s\n\n' "$total" "$dialect"

    for path in "${paths[@]}"; do
        idx=$((idx + 1)); name=$(basename "$path" .sql); t0=$SECONDS
        out=$(sqlfluff lint --config "$config" --templater "$templater" --dialect "$dialect" "$path" 2>&1)
        status=$?
        case $status in
            0) mark="✔"; pass=$((pass + 1)) ;;
            1) mark="✘"; fail=$((fail + 1)) ;;
            *) mark="⚠"; err=$((err + 1)) ;;
        esac
        printf '  %s %-50s (%d/%d · %ds)\n' "$mark" "$name" "$idx" "$total" "$((SECONDS - t0))"
        if [[ $status -ne 0 ]]; then
            failed+=("$name")
            printf '\n%s %s  (%s)\n%s\n' "$mark" "$name" "$path" "$out" >> "$details"
        fi
    done

    echo ""
    if [[ ${#failed[@]} -gt 0 ]]; then
        echo "─────────────── issues ───────────────"
        cat "$details"
        echo ""
    fi
    rm -f "$details"

    printf '[dbt-lint] finished in %ds · ✔ %d  ✘ %d  ⚠ %d  (of %d)\n' \
        "$((SECONDS - lint_start))" "$pass" "$fail" "$err" "$total"
    if [[ ${#failed[@]} -eq 0 ]]; then
        echo "[dbt-lint] ✔ compatible with $dialect"
    else
        echo "[dbt-lint] ✘ NOT compatible with $dialect — failed: ${failed[*]}"
        return 1
    fi
}