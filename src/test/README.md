# Dameng connector tests

Run tests with the Maven wrapper from the repository root, using JDK 21 or newer:

```sh
./mvnw test
./mvnw verify -Pdameng-tests
./mvnw verify -Pdameng-tests \
  -Dit.test=DamengPostgresCompatibilityIT -Dfailsafe.failIfNoSpecifiedTests=false \
  -Ddameng.compatible.mode=7
```

The first command runs unit tests; the second also runs routine database and embedded-engine integration tests. The third runs all 16 PostgreSQL-mode FLOAT_MODE/NUMBER_MODE/ORA_DATE_FMT/BACKSLASH_ESCAPE combinations, covering snapshot, historical DDL, CRUD and recovery. Use `JAVA_HOME=/path/to/jdk` before the command if the shell defaults to an older JDK.

ORA_DATE_FMT is effective only in Oracle mode. Other profiles do not read, validate or include it in compatibility signatures;
their matrices retain its variation as a control. Unit tests verify missing and arbitrary reported values are ignored.
LENGTH_IN_CHAR is not read, validated or tracked in any mode; VARCHAR values remain strings regardless of the declared length unit.

Database tests require Docker and the documented DM image. They create and clean up their own containers unless an external test database is explicitly configured. Integration tests are skipped by default.

| Scope | Maven profile | Selection |
| --- | --- | --- |
| Routine database/engine regression | `dameng-tests` | Excludes probes, performance experiments and independent Connect runtime tests |
| Independent Kafka Connect runtime | `dameng-connect-tests,assembly` | `DamengKafkaConnectIT`; packages the current connector |
| Manual capability/dependency diagnostics | `dameng-probes` | Only `*ProbeIT` |
| Performance experiments | `dameng-performance-tests,assembly` | Only `*BenchmarkIT` and `*PerformanceIT`; check each experiment's opt-in properties |
| All Failsafe tests except independent Connect runtime | `dameng-all-tests` | Includes diagnostics and performance; opt-in conditions still apply |

The Connect test profile assembles the Kafka runtime resolved by the pinned Debezium parent. It aligns Jetty to `12.1.11` with the Jetty and EE10 BOMs because this parent manages only the servlet artifact at that version, while Kafka `4.3.1` requests `12.0.34` for the other Jetty components. Keep this test-only alignment consistent when updating the parent. Jetty is not part of the connector plugin archives.

Add `kafka2-compatible` to verify the packaged connector with Kafka Connect worker `3.9.1`:

```sh
./mvnw clean verify -Pdameng-connect-tests,assembly,kafka2-compatible \
  -Dit.test=DamengKafkaConnectIT
```

The compatibility profile overrides Kafka dependencies and uses Kafka 3.9.1's Jetty `9.4.57.v20241219` for the assembled runtime. It also manages Commons IO at `2.20.0` so Testcontainers' Commons Compress can copy files into containers despite the older broker dependency. The tests select the archive with the `-kafka2-compatible` suffix and check the worker's version through REST before checking plugin discovery. Docker and the prepared DM image are required. The worker JVM defaults to Java 21; set `-Ddameng.connect.java.image=<image:tag>` for another compatible Java image.

On 2026-11-18, the compatibility package passed 14,362 unit cases and 2 architecture checks. All 3 `DamengKafkaConnectIT` cases passed on the Maven-assembled Kafka `3.9.1` runtime, Java 21 and DM `8.1.3.162`: snapshot/CRUD with task and worker recovery, all 3,000 rows after a worker kill during transaction publication, and typed CRUD through a broker outage and worker reassignment. These results cover the assembled runtime; an unmodified Kafka distribution image was not tested in this run.

Run one diagnostic explicitly, for example:

```sh
./mvnw verify -Pdameng-probes \
  -Dit.test=DamengDdlSyntaxProbeIT -Dfailsafe.failIfNoSpecifiedTests=false
```

Shared configuration, SQL samples and datatype inputs live in `DamengTestConfiguration`, `DamengDdlTestCases` and `DamengDatatypeCases`. Executable test classes should not provide fixtures to other test classes. Large suites use JUnit `@Nested` groups by behavior while sharing their enclosing fixture.

Select a complete nested suite with `-Dit.test=DamengConnectorIT`. Run method-filtered selections separately: mixing a nested suite name with another class's `#method` filter in one `it.test` value can omit the nested suite. Check the executed class names and counts in the XML reports.

Surefire writes unit reports under `target/surefire-reports`; Failsafe writes integration reports under `target/failsafe-reports`. Use a clean run when collecting coverage with `dameng-coverage` so previous JaCoCo data is not included. A targeted run does not establish complete regression coverage.

The 95% line coverage gate measures handwritten production classes. Locally generated ANTLR classes under `io/debezium/ddl/parser/dameng/generated` are excluded from the report and gate; the handwritten `DamengSqlLexerBase` remains included.

`DamengPublicDdlParserTest` exercises the standard Debezium `DdlParser` interface: ordered change events, atomic model updates, parser reuse after failure, structured error positions, original quoted defaults, and selected-token planning. Run it with `./mvnw process-sources test -Dtest=DamengPublicDdlParserTest`. Existing DDL, scope and compatibility tests also exercise the public parse lifecycle through the connector entry points. `DamengAntlrStreamingIT` verifies DDL replay and schema recovery against a real database; packaged Connect tests verify that the public parser dependency is available in the deployed plugin.

See the [Dameng manual](../../documentation/dameng.adoc#dameng-testing) for images, offline preparation and coverage checks. [testing.adoc](testing.adoc) contains the applicability matrix and performance properties; [regression-history.adoc](regression-history.adoc) preserves dated execution records.

## Standalone migration regression

Recorded on 2026-11-17, in order:

| Stage | Official upstream commit | Debezium | Unit + architecture | Selected DB + Connect |
| --- | --- | --- | --- | --- |
| Independent baseline | `ea91582751a6f8515a5893d9ae3f45faf4a74116` | `3.6.4-SNAPSHOT` | 14,357 passed | 20 passed |
| Main alignment | `ed2fdb9f9fce824cff587ded8c96217b10ef3187` | `3.8.0-SNAPSHOT` | 14,357 passed | 20 passed |

Both stages used clean, unmodified upstream sources and separate Maven repositories, with zero failures, errors or skipped cases in the final selected reports. The baseline project and reports were archived before changing framework versions. Build JDK was 25.0.4; connector classfiles target Java 17. The database fixture was DM `8.1.3.162`, full build `03134284194-20240621-232765-20108`, image `dameng:8.1.3.162-20240621-rev232765`, with JDBC driver `8.1.3.140`. These selected integration cases used native compatibility mode.

The packaged Connect checks used Java 21 and resolved Kafka runtimes: `4.3.0` for the baseline, and `4.3.1` with Jetty `12.1.11` alignment for main. They verified service-loader plugin discovery, initial snapshot and CRUD, task stop/resume, replacement of a killed worker, and recovery of offline DDL/DML from Kafka offsets and schema history. This is not certification of an unmodified Kafka distribution, the Docker tooling's default `apache/kafka:3.9.1` image, or the historical Kafka 2.0 deployment.

The main upgrade required configuration registration API changes and three test fixture/assertion updates; it did not patch framework sources or change Dameng consumption positions. Main's inherited configuration options follow that pinned framework. Checkstyle and plugin archive checks passed in both stages. The full database compatibility matrices, performance suites and complete 95% coverage gate were not rerun for this migration.

A separate JaCoCo scope check passed 134 grammar cases plus 2 architecture cases. Its XML report excludes the generated parser package and includes the handwritten lexer with executed lines. This check used `-Pdameng-coverage -DskipITs -Djacoco.skipCheck=true -Dtest=DamengAntlrGrammarTest` with fresh, separate coverage data and test report paths; it validates the report scope, not the 95% threshold.

The migration uses the same selected database checks before and after the framework upgrade. After building the pinned core dependencies, run these serially from the repository root:

```sh
./mvnw clean process-sources verify -Passembly
python3 .github/scripts/verify-plugin.py
./mvnw -Pdameng-tests failsafe:integration-test failsafe:verify \
  '-Dit.test=DamengConnectorIT*#shouldSnapshotThenStreamCommittedCrud+shouldCaptureNewColumnAndRestoreSchemaAfterRestart' \
  -Ddameng.failsafe.reports.directory=target/failsafe-migration-snapshot-ddl
./mvnw -Pdameng-tests failsafe:integration-test failsafe:verify \
  -Dit.test=DamengCaptureScopeIT,DamengIncrementalSnapshotIT \
  -Ddameng.failsafe.reports.directory=target/failsafe-migration-scope-incremental
./mvnw -Pdameng-tests failsafe:integration-test failsafe:verify \
  '-Dit.test=DamengStandbyIT#shouldSnapshotAndStreamOnStandbyWithPrimarySignals' \
  -Ddameng.failsafe.reports.directory=target/failsafe-migration-standby
./mvnw package -Pdameng-connect-tests,assembly -DskipTests
./mvnw -Pdameng-connect-tests,assembly failsafe:integration-test failsafe:verify \
  '-Dit.test=DamengKafkaConnectIT#shouldRecoverPackagedConnectorAfterTaskRestartAndWorkerKill' \
  -Ddameng.failsafe.reports.directory=target/failsafe-migration-connect
```

The expected integration counts are 2 snapshot/DDL cases, 15 capture-scope/incremental cases, 2 standby watermark cases (`insert_insert` and `insert_delete`), and 1 packaged Connect recovery case. The standby cases include connector restart, role switchover and standby restart. Direct Failsafe goals reuse the classes compiled by the preceding build; recompile after any Java changes. A separate report directory for each selection preserves the executed cases.

## Compatibility matrices

Native compatibility regression uses `-Pdameng-tests -Dit.test=DamengNativeCompatibilityIT,DamengLexicalSettingsIT -Ddameng.compatible.mode=0`.
`DamengNativeCompatibilityIT` owns disposable databases and covers all 16 FLOAT_MODE/NUMBER_MODE/ORA_DATE_FMT/BACKSLASH_ESCAPE combinations.
It checks binary/decimal FLOAT aliases, pure DATE, compiled string defaults, snapshot, archived CREATE/ALTER and CRUD,
NULL transitions, deleted rows and two restarts against independently read JDBC values and metadata.
On the current DM 8.1.3.162 fixture, NUMBER_MODE=1 selects decimal FLOAT and BACKSLASH_ESCAPE=1 enables escape decoding;
FLOAT_MODE and ORA_DATE_FMT do not change the sampled values. Native BACKSLASH_ESCAPE=1 also observes the session's actual
raw/escaped behavior and records it as derived NATIVE_BACKSLASH_SEMANTICS, preserving the raw behavior of DM 8.1.2.128.
The native signature now includes NUMBER_MODE;
older native offsets without this parameter require a new capture baseline.
With NUMBER_MODE=1, derived NATIVE_REAL_SEMANTICS records the actual CAST metadata: decimal REAL on 8.1.3.162,
binary REAL on 8.1.2.128. Both derived settings are checked at mining boundaries without an extra query per boundary.
The lexical test also checks DISQL, bound control characters and nested comments.
The same lexical test can be selected on the older DM 8.1.2.128 image to verify its raw behavior.
One-off parameter probes remain outside the repository.

Oracle compatibility regression uses `-Pdameng-tests -Dit.test=DamengOracleCompatibilityIT,DamengCaseSensitivityIT -Ddameng.compatible.mode=2`.
`DamengOracleCompatibilityIT` owns disposable databases and covers all 16 FLOAT_MODE/NUMBER_MODE/ORA_DATE_FMT/BACKSLASH_ESCAPE combinations.
Isostring/adaptive time modes and both nested-comment settings are distributed across the matrix; each NUMBER_MODE/BACKSLASH_ESCAPE combination covers both time modes.
Add `DamengOracleNumberModeIT` with `-Ddameng.test.ini.NUMBER_MODE=1` to verify decimal FLOAT values and defaults, database precision rounding, offline DDL/CRUD and recovery in precise, string and double decimal modes.
The dedicated engine test checks timestamp DATE values and defaults, numeric aliases, archived CREATE/ALTER and CRUD, NULL transitions and restart recovery in isostring/adaptive modes.
String images and schema defaults are checked independently against JDBC, including plain/N/E/Q literals,
single/multi-column ADD, ALTER DEFAULT and bound literal backslashes. Inline comments in archived CREATE statements
are also checked against JDBC; current snapshot metadata does not capture column comments. One-off diagnostic scripts belong outside the repository.

MySQL compatibility regression uses `-Pdameng-tests -Dit.test=DamengMySqlCompatibilityIT -Ddameng.compatible.mode=4`.
The test owns disposable databases and covers all 16 FLOAT_MODE/NUMBER_MODE/ORA_DATE_FMT/BACKSLASH_ESCAPE combinations.
CASE_SENSITIVE, nested comments and MY_STRICT_TABLES each take both values across the matrix.
Optionally fix strict mode with `-Ddameng.test.ini.MY_STRICT_TABLES=0` or `1`.
It checks backticks, escaped names, auto-increment keys, DATETIME, high-precision decimals, TEXT/BLOB aliases, defaults,
archived CREATE/ALTER and CRUD, decimal FLOAT, escaped defaults, literal backslashes in bound parameters,
NULL transitions, deleted rows and two restarts against independently read JDBC values.

PostgreSQL compatibility regression uses the third command above and owns its disposable databases.
It checks both CASE_SENSITIVE and nested-comment settings across all 16 parameter combinations, quoted identifiers,
TIME/TIMESTAMP WITHOUT TIME ZONE, decimal FLOAT and precision changes, plain/Q/N/E defaults, ALTER DEFAULT,
bound literal backslashes, archived CREATE/ALTER and CRUD, NULL transitions, deleted rows and two restarts.
Expected images are read independently through JDBC before deletion. One-off probes stay outside the repository.
