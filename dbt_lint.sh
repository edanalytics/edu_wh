dbt_lint() {
    local templater="raw"       # compiled SQL is plain SQL
    local dialect="snowflake"   # override with --dialect databricks or other to check compatibility

    while [[ "$1" == --* ]]; do                # consume any leading --flag value pairs
        case "$1" in
            --templater) templater="$2"; shift 2 ;;
            --dialect)   dialect="$2";   shift 2 ;;
        esac
    done

    [[ $# -eq 0 ]] && { echo "Usage: dbt_lint [--dialect X] [--templater X] model1 [model2 ...]"; return 1; }

    echo "Compiling: $*"
    local compile_log
    compile_log=$(dbt compile --select "$@" --quiet 2>&1) || {
        echo "dbt compile failed:"
        echo "$compile_log"
        return 1
    }

    local paths=()
    for model in "$@"; do
        local found
        found=$(find -L ./target/compiled -name "${model}.sql" 2>/dev/null | head -1)
        [[ -z "$found" ]] && { echo "ERROR: compiled file not found for '${model}'"; continue; }
        paths+=("$found")
    done

    [[ ${#paths[@]} -eq 0 ]] && return 1

    local config
    config="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.sqlfluff"

    echo "Linting: ${paths[*]}"
    sqlfluff lint \
        --config "$config" \
        --templater "$templater" \
        --dialect "$dialect" \
        "${paths[@]}"

    if [[ $? -eq 0 ]]; then
        echo "Compatible with $dialect"
    else
        echo "Not compatible with $dialect. See errors above!"
    fi
}
