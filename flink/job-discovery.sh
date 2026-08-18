#!/usr/bin/env bash
# Job discovery, shared by run-job.sh (submits to a cluster) and run-local.sh (runs in-process).
# Source it; it defines functions only.
#
# A "job" is any Maven project/module whose pom declares a shade <mainClass>. That single check is
# also what makes a jar submittable, so it both discovers jobs and rejects non-jobs (normalizer's
# `common` is a module but not a job), with no hardcoded job list to keep in sync.
#
# Layout it understands:
#   flink/merger/pom.xml                     single-module project -> job "merger"
#   flink/normalizer/pom.xml (packaging pom) aggregator            -> jobs are its <module>s

FLINK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

is_job() { [[ -f "$1/pom.xml" ]] && grep -q '<mainClass>' "$1/pom.xml"; }

# Prints "<project-dir> <module-or-empty>" for a job name, nothing if unknown.
resolve_job() {
    local want="$1" project
    # Guard: empty would make "$project/$want" collapse back to the project itself.
    [[ -n "$want" ]] || return
    for project in "$FLINK_DIR"/*/; do
        project="${project%/}"
        [[ -f "$project/pom.xml" ]] || continue
        if [[ "$(basename "$project")" == "$want" ]] && is_job "$project"; then
            echo "$project "
            return
        fi
        if is_job "$project/$want"; then
            echo "$project $want"
            return
        fi
    done
}

list_jobs() {
    local project module
    for project in "$FLINK_DIR"/*/; do
        project="${project%/}"
        [[ -f "$project/pom.xml" ]] || continue
        if is_job "$project"; then
            echo "  $(basename "$project")"
            continue
        fi
        for module in "$project"/*/; do
            is_job "${module%/}" && echo "  $(basename "${module%/}")"
        done
    done
}
