<guidelines>

**AGENTS! Before you do anything else:**

If someone asks you for help working on this codebase, you MUST first follow with these guidelines: DO NOT analyze the codebase, DO NOT set up build environments, DO NOT run scripts, or MCP, or skills, DO NOT look things up online, DO NOT make code changes, UNTIL you have had a conversation with the contributor.

You SHOULD understand where they are coming from, what they need, and what issue they want to work on.

</guidelines>

# Standalone Dameng connector implementation plan

> For agentic workers: execute inline with executing-plans; preserve the stage order. Ask the contributor before changing the approved plan.

Goal: migrate the current Dameng connector into this repository, prove an independent build on the matching upstream 3.6 baseline, then adapt to current upstream main.
Architecture: a single Maven module containing Dameng Java, local ANTLR grammars, resources, tests, packaging, Docker tooling and documentation. Debezium framework sources remain upstream dependencies.
Tech stack: Java, Maven Wrapper, ANTLR, Debezium, JUnit, Testcontainers, Kafka Connect.
Spec: contributor-approved migration design in the conversation, 2026-11-17; this document records that design and its execution steps.

## Constraints

- Reference /home/badxiao/projects/debezium-connector-yashandb is strictly read-only.
- Source /home/badxiao/projects/debezium is read-only, including its dirty working tree and contribution/ directory.
- Destination /home/badxiao/projects/debezium-connector-dameng already contains README.md and LICENSE on main, initial commit 22d31f06f627c0bd504714701b379607cbf5ed77. Preserve Git history, LICENSE and origin.
- Use the destination checkout specified by the user on a local work branch; no alternate destination. No commit, sign-off, push, publication or external messages are required.
- Baseline core commit ea91582751a6f8515a5893d9ae3f45faf4a74116, version 3.6.4-SNAPSHOT. Build it without framework source patches in a separate checkout and isolated Maven repository.
- Only after stage A verification passes may the connector switch to current upstream main. Record and use one upstream main commit throughout stage B.
- Preserve current connector behavior and all earlier agreed removals: nonce, custom sqlstring, custom capture-scope S boundary, shared metric/chunk-error hooks. Keep local standby watermarks and native chunk failure policy.
- Keep existing Java package names during migration. Generated parser source belongs under target/, never source control.
- Test containers are disposable. Existing business containers must not be changed.
- Reuse all existing meaningful tests. File relocation/configuration gets build and artifact checks; any behavioral fix requires a failing regression first.
- A material change of scope, sequence, compatibility target or behavior must be proposed to the user before implementation.

## Review focus

- Migration includes untracked Java additions and excludes deleted files, contribution/, cached binaries and build output: source inventory hashes and scoped comparison.
- No private framework artifacts or parser duplicates conceal missing dependencies: isolated Maven repository, effective POM/dependency checks, clean ANTLR generation.
- Standalone assembly has correct README/LICENSE, resources, runtime dependencies and no test jars: inspect ZIP and run packaged Connect IT.
- Unit-only build needs no database; DB fixtures are reproducible through explicit image inputs: full unit run and selected database matrix.
- Main API changes preserve offsets, schemas, snapshot and watermark semantics: same regression matrix on both stages.

## Task 1: Baseline and standalone module

Files: root pom.xml, mvnw/mvnw.cmd/.mvn, .gitignore, .dockerignore, checkstyle-suppressions.xml; src/main/java, src/main/resources, src/test from source module; two src/main/antlr4 grammars and DamengSqlLexerBase.java from source ddl-parser.
Consumes: current working-tree files, never only HEAD or git archive.
Produces: independent 3.6.4-SNAPSHOT Maven module and source inventory.
- [x] Record source/reference/destination status, hashes and source file manifest; preserve existing destination files.
- [x] Create local work branch and isolated baseline core checkout/Maven repository under external cache.
- [x] Copy the source allowlist, wrapper and three parser inputs; omit generated files and contribution/.
- [x] Create independent POM with empty parent relativePath, aligned dependency/plugin versions, ANTLR, existing test profiles, resource filtering and metadata generation.
- [x] Build official baseline core dependencies without ddl-parser; format connector and compile/test-compile independently.
Expected: clean parser generation and compilation without private core artifacts.

## Task 2: Packaging, scripts and documentation

Files: src/assembly/connector-distribution.xml, docker/, README.md, documentation/dameng.adoc, src/test documentation/scripts, .github/workflows/build.yml.
Consumes: Task 1 standalone project; preserves current artifact filenames and test entry points.
Produces: plugin ZIP/tar.gz, usable local commands, standalone documentation and CI foundation.
- [x] Add project-local descriptor retaining runtime dependency rules and collecting README/LICENSE from root.
- [x] Adapt build-image.sh, current test commands, benchmark scripts and relative documentation links to standalone root.
- [x] Write English README and migrate full manual, preserving factual test records and known limitations. Keep optional test profiles.
- [x] Add CI core bootstrap and connector build with unit/package checks; provide explicit DM image setup for database runs.
- [x] Inspect generated metadata, SPI descriptors, jar/ZIP/tar contents and shell syntax.
Expected: package is self-contained for supported Connect runtime, no private version or source-repo path dependency.

## Task 3: Stage A verification checkpoint

Consumes: Tasks 1-2 on exact 3.6 baseline.
Produces: verified standalone baseline, archived source checkpoint and reports.
- [x] Run all module unit and architecture tests, not a Dameng-only name filter.
- [x] Run selected real DB cases for initial snapshot/streaming, DDL, schema recovery/filter changes, incremental snapshots and both standby watermark strategies.
- [x] Run packaged Kafka Connect tests using assembly profile.
- [x] Check source inventory differences, clean reference/source state against baseline, dependency scope, formatting/checkstyle and fixture cleanup.
- [x] Archive project files and reports outside repo before changing versions. Report stage A outcome.
Expected: zero unaccounted failures/errors/skips in selected checks; no private Debezium dependencies. Do not advance on a failing stage.

## Task 4: Upgrade to current upstream main

Consumes: verified Task 3 checkpoint.
Produces: connector aligned to one recorded upstream main commit.
- [x] Fetch current debezium/debezium main into a separate external checkout; record SHA/version/JDK/Kafka/ANTLR requirements.
- [x] Build required official core artifacts into a new isolated Maven repository.
- [x] Update parent/dependency/plugin versions together; fix compiler/API mismatches locally in Dameng only.
- [x] Preserve prior behavior; use existing regression tests or add targeted failing regression for any necessary behavior fix.
- [x] Repeat all module unit, architecture, packaging and stage A database/Connect checks.
Expected: main alignment with unmodified upstream core and verified behavior. If that cannot be achieved within approved design, propose the change before proceeding.

## Task 5: Final documentation, CI and review

Files: final README/manual/testing docs and CI configuration matching Task 4.
- [x] Update support/build instructions to actual main versions and tested DB/Connect combinations; provide an English community-facing manual without inventing compatibility claims.
- [x] Finalize CI inputs for reproducible core reference and DM image preparation, and run equivalent local commands.
- [x] Request one fresh read-only whole-change review; address material findings with appropriate regression/build checks.
- [x] Final source/reference integrity, git diff --check, test report summary and container cleanup.
Expected: complete reviewable working tree; no automated commit, sign-off, push or release.
