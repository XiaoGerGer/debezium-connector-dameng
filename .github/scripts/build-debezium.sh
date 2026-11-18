#!/usr/bin/env bash
# Copyright Debezium Authors.
# Licensed under the Apache Software License, Version 2.0.
set -euo pipefail

project_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
core_ref=$(tr -d '\r\n' < "$project_dir/.github/debezium-core-ref")
[[ "$core_ref" =~ ^[0-9a-f]{40}$ ]] || { echo 'Invalid pinned Debezium commit' >&2; exit 1; }
[[ $# -ge 1 ]] || { echo 'Usage: build-debezium.sh CORE_CHECKOUT [Maven options]' >&2; exit 1; }
core_dir=$1
shift
if [[ ! -d "$core_dir" ]]; then
    git init "$core_dir"
    git -C "$core_dir" fetch --depth=1 https://github.com/debezium/debezium.git "$core_ref"
    git -C "$core_dir" checkout --detach FETCH_HEAD
fi
[[ "$(git -C "$core_dir" rev-parse HEAD)" == "$core_ref" ]] || { echo 'Core checkout does not match the pinned commit' >&2; exit 1; }
[[ -z "$(git -C "$core_dir" status --porcelain)" ]] || { echo 'Core checkout must be unmodified' >&2; exit 1; }
cd -- "$core_dir"
./mvnw -B -ntp \
    -pl debezium-bom,debezium-connector-common,debezium-ddl-parser,debezium-util,debezium-config,debezium-embedded,debezium-storage/debezium-storage-kafka,debezium-storage/debezium-storage-file,debezium-connect-plugins,debezium-schema-generator,support/archunit,support/ide-configs,support/checkstyle,support/revapi \
    -am process-sources install -DskipTests -DskipITs -Darchunit.skip=true \
    -Dformat.formatter.goal=validate -Dformat.imports.goal=check "$@"
