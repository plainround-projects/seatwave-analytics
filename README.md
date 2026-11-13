# seatwave-analytics

Module 31 ("Batch and stream analytics") of the course System Design Fundamentals, running case SeatWave. A stream job counts tickets sold per show per minute of event time; a nightly batch loads the same bookings into a warehouse table; a reconciliation proves the two agree for every show and minute once the late records are accounted for.

The runs below were made on the code of the commit that holds this README (13 November 2026).

## Prerequisites

- JDK 25 (the build uses a toolchain; Eclipse Temurin 25.0.2 on the verification machine, the newest is 25.0.5).
- For the stack only: Docker with Compose (Docker 24.0.6 and Compose v2.23.0 on the verification machine). `./gradlew check` needs no Docker.

## Dependencies (each checked on Maven Central and the project's release page on 13 November 2026)

| Coordinates | License | Why the JDK alone is not enough |
|---|---|---|
| `org.apache.kafka:kafka-streams:4.4.0` | Apache License 2.0 | windows, state stores, repartitioning and transactional commits over Kafka |
| `at.yawk.lz4:lz4-java:1.14.0`, `org.xerial.snappy:snappy-java:1.1.10.12` (constraints) | Apache License 2.0 | kafka-clients 4.4.0 asks for 1.11.2 and 1.1.10.7, which OSV lists with advisories |
| `org.duckdb:duckdb_jdbc:2.0.0.1` (DuckDB 2.0.0) | MIT License | an embedded columnar SQL warehouse; it also writes the lake's Parquet files, so no Parquet library is needed |
| `com.h2database:h2:2.5.252` | MPL 2.0 or EPL 1.0 | the embedded row store the column store is measured against |
| `com.fasterxml.jackson.core:jackson-databind` (BOM 2.23.0) | Apache License 2.0 | Module 28's events are JSON |
| `org.slf4j:slf4j-api:2.0.20`, `ch.qos.logback:logback-classic:1.6.6` | MIT; EPL 1.0 or LGPL 2.1 | logging of the Kafka clients |
| `org.junit:junit-bom:6.1.3`, `org.assertj:assertj-core:3.27.7`, `org.apache.kafka:kafka-streams-test-utils:4.4.0` (tests) | EPL 2.0; Apache License 2.0 | a test runner, readable assertions, the topology test driver |
| Image `apache/kafka:4.4.0@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f` | Apache License 2.0 | the broker (the vendor's image, amd64 and arm64) |

Every coordinate of both lockfiles was checked against OSV, with a control that must be found:

```text
$ python3 -I scripts/osv-check.py
coordinates checked: 36; with advisories: 0
control at.yawk.lz4:lz4-java:1.11.2: 3 advisories found (GHSA-343h-94h5-c4wr, GHSA-gm45-99xc-r7wv, GHSA-mcr4-qmvw-px4g)
```

## Build, test and the four programs (no Docker)

```text
> Task :check
> Task :jacocoTestReport
Deprecated Gradle features were used in this build, making it incompatible with Gradle 10.
You can use '--warning-mode all' to show the individual deprecation warnings and determine if they come from your own scripts or plugins.
For more on this, please refer to https://docs.gradle.org/9.9.1/userguide/command_line_interface.html#sec:command_line_warnings in the Gradle documentation.
BUILD SUCCESSFUL in 6s
10 actionable tasks: 10 executed
Consider enabling configuration cache to speed up this build: https://docs.gradle.org/9.9.1/userguide/configuration_cache_enabling.html
tests run by ./gradlew check: 24 in 6 classes, 0 failed
line coverage of the code check measures (both source sets, without the programs and StackClients): 707/784 = 90.2% (floor 80%)
$ ./gradlew spotlessCheck --rerun-tasks
> Task :spotlessJava
> Task :spotlessJavaCheck
> Task :spotlessCheck
BUILD SUCCESSFUL in 507ms
$ ./gradlew windowsRun checkpointRun reconcileRun layoutRun   (each twice, compared with the outputs the module captured from the seed)
windowsRun: 55 lines, byte-identical to the seed's capture, second run identical
checkpointRun: 8 lines, byte-identical to the seed's capture, second run identical
reconcileRun: 44 lines, byte-identical to the seed's capture, second run identical
layoutRun: 6 lines, byte-identical to the seed's capture, second run identical
./gradlew test --rerun-tasks, 40 runs beside two busy loops: 0 failed
$ ./gradlew check, from a path of 377 characters: BUILD SUCCESSFUL
$ ./gradlew check, from a folder whose name has a space: BUILD SUCCESSFUL
```

`./gradlew windowsRun checkpointRun reconcileRun layoutRun` print the module's teaching outputs; each program needs a 3 GB heap (set on its task), because it holds the whole on-sale in memory.

## Mutation check

Each mutant removes one rule from a copy of the project; each must make the test that guards the rule fail (`scripts/mutants.sh`):

```text
$ scripts/mutants.sh
nodedupe (a repeated message_id is counted again): tests failed: ReconciliationTest.streamAndBatchAgreeOnEveryMinuteOnceTheSideOutputIsReplayed
nocorrection (the side-output replay adds nothing): tests failed: ReconciliationTest.streamAndBatchAgreeOnEveryMinuteOnceTheSideOutputIsReplayed
sameversion (a repeated version rewrites the row): tests failed: ReconciliationTest.loadingTheSameDayTwiceLeavesTheSameRows
processing-time (windows are keyed by the send time instead of occurred_at): tests failed: SalesPerMinuteTopologyTest.countsAConfirmInTheMinuteItOccurredNotTheMinuteItWasSent, SalesPerMinuteTopologyTest.countsEveryConfirmOnceOrKeepsItOnceInTheLateTopic, SalesPerMinuteTopologyTest.sendsAConfirmAfterTheGraceToTheLateTopicKeyedByItsWindow
late-dropped (a record after the grace is dropped by the count with no trace): tests failed: SalesPerMinuteTopologyTest.countsEveryConfirmOnceOrKeepsItOnceInTheLateTopic, SalesPerMinuteTopologyTest.sendsAConfirmAfterTheGraceToTheLateTopicKeyedByItsWindow
unparseable-stops (an unparseable record stops the stream instead of going to the dead-letter topic): tests failed: BookingEventJsonTest.refusesValuesThatAreNotOneJsonObject, SalesPerMinuteTopologyTest.parksAnUnreadableRecordInTheDeadLetterTopicAndKeepsCounting
load-twice (loading a partition again rewrites every row): tests failed: WarehouseTest.loadingTheSameDayAgainChangesNoRow
mutants: 7, survivors: 0
```

## The stack (Docker)

`scripts/stack.sh run --run` starts one broker (`compose.yaml`, port 23192 on 127.0.0.1, a 384 MB heap in a 768 MB container), runs the stream job, the lag probe and the publisher (a tenth of the seeded on-sale, 137,699 records with the stalled shard-37 publisher and 3 unparseable records, at 1,500 records a second), stops the job when it has caught up, archives the topic into `lake/`, loads the day twice into DuckDB and reconciles; `scripts/stack.sh crash --run` does the same but kills the stream job by its process id 40 seconds into the publish and starts it again 5 seconds later. Both end with `down -v`. Measured under the course's measurement lock, with the programs on the host and the broker in Docker Desktop's VM (every request crosses that boundary).

Run with the stalled publisher, exactly-once on:

```text
$ scripts/stack.sh run --run --yes   (under the measurement lock; load average before: 1.60 1.69 2.00)
published 137,699 records (135,000 confirms, 2,699 cancels) and 3 unparseable to seatwave.bookings in 91.7 s: 1,501 records/s (planned 1,500); send failures 0
bookings-archiver: archived 137,699 records into lake/bookings (28 Parquet files, 6,609,324 bytes); skipped 3 unreadable; 2.8 s before the 15 s quiet period
nightly-bookings-load 2026-11-20: 137,699 records read, 135,000 rows inserted or updated, 135,000 rows in fact_bookings; 0.92 s
nightly-bookings-load 2026-11-20: 137,699 records read, 0 rows inserted or updated, 135,000 rows in fact_bookings; 0.85 s
largest lag 3,600 records at t=56 s = 2.4 s at 1,500 records/s (alert at 120 s)
lake/bookings: 6504 kB on disk
sales-reconciliation: 1,050 windows (show and minute) compared; stream 134,749 confirms, late topic 251, warehouse 135,000
  before the correction: 119 windows differ by 251 confirms; 119 explained by the late topic; 0 windows where the stream counted more than the warehouse
  after the correction:  AGREED, 0 windows differ
    late confirms of minute 19:07: 131
    late confirms of minute 19:08: 120
  dead-letter topic seatwave.bookings.dlq: 3 records the reader could not use
nothing of seatwave-analytics is left (containers, networks, volumes)
```

Run with the stream job killed mid-window and restarted:

```text
$ scripts/stack.sh crash --run --yes   (under the measurement lock; load average before: 1.47 1.65 1.93)
KILL of the stream job, pid N, 40 s into the publish
restarted as a new process after 5 s
published 137,699 records (135,000 confirms, 2,699 cancels) and 3 unparseable to seatwave.bookings in 91.7 s: 1,501 records/s (planned 1,500); send failures 0
bookings-archiver: archived 137,699 records into lake/bookings (28 Parquet files, 6,608,915 bytes); skipped 3 unreadable; 2.9 s before the 15 s quiet period
nightly-bookings-load 2026-11-20: 137,699 records read, 135,000 rows inserted or updated, 135,000 rows in fact_bookings; 0.99 s
nightly-bookings-load 2026-11-20: 137,699 records read, 0 rows inserted or updated, 135,000 rows in fact_bookings; 0.96 s
largest lag 69,782 records at t=85 s = 46.5 s at 1,500 records/s (alert at 120 s)
lake/bookings: 6504 kB on disk
sales-reconciliation: 1,050 windows (show and minute) compared; stream 88,248 confirms, late topic 46,752, warehouse 135,000
  before the correction: 449 windows differ by 46,752 confirms; 449 explained by the late topic; 0 windows where the stream counted more than the warehouse
  after the correction:  AGREED, 0 windows differ
    late confirms of minute 19:04: 3
    late confirms of minute 19:05: 35
    late confirms of minute 19:06: 3,717
    late confirms of minute 19:07: 8,670
    late confirms of minute 19:08: 8,620
    late confirms of minute 19:09: 8,586
    late confirms of minute 19:10: 8,645
    late confirms of minute 19:11: 8,476
  dead-letter topic seatwave.bookings.dlq: 3 records the reader could not use
nothing of seatwave-analytics is left (containers, networks, volumes)
```

Row store against column store, the whole on-sale in memory (`./gradlew layoutDbRun`):

```text
$ ./gradlew --quiet layoutDbRun   (under the measurement lock)
load before: 1.43 1.67 1.88
fact_bookings: 1,350,000 rows; query: confirmed tickets per show per minute; 5 runs each, median
  H2 2.5.252 (row store, in memory):       load 4.5 s, query 268.7 ms
  DuckDB 2.0.0 (column store, in memory):  load 2.6 s, query 24.1 ms
  the row store took 11.2 times as long; same answer: true (1,050 windows, 1,323,167 confirmed)
load after: 1.37 1.65 1.87
```

Nothing of the project is left after the runs:

```text
$ docker ps -a / network ls / volume ls --filter label=com.docker.compose.project=seatwave-analytics
containers: 0
networks: 0
volumes: 0
```

## Project structure

```text
src/main/java/.../analytics/        the JDK-only model and its four programs (Module 31's seed)
kafka/src/main/java/.../kafka/      the stream job, the reader, the publisher, the archiver, the lag probe
kafka/src/main/java/.../warehouse/  the lake writer, the warehouse load, the reconciliation, the layout comparison
warehouse/fact_bookings.sql         the table, the nightly load and the view
src/test/java/                      the tests (no Docker)
scripts/                            stack.sh, mutants.sh, osv-check.py
compose.yaml                        one Kafka broker and the topic setup
```

## What is simplified

- One broker with one replica per partition; Module 27 shows replication.
- The stream job does not dedupe by `message_id`: exactly-once processing absorbs redeliveries, and a record republished upstream would show as an overcount in the reconciliation.
- The broker allows timestamps up to 30 days ahead, because the seeded on-sale is dated 20 November 2026, after the day the stack ran.
- The stack runs a tenth of the on-sale; the arithmetic to the full size is in the module.

## No CI on purpose

There is no `.github/` folder: the course verifies every project on the verification machine and records the runs here.
