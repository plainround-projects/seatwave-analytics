# Changelog

## 1.0.0 (13 November 2026)

- Built Module 31's project from its code seed: the seed's model and its four programs unchanged (their outputs through Gradle are byte-identical to the module's captured ones); the generator, the event and the delivery made public for the second source set.
- Added the `kafka` source set: the Kafka Streams job `analytics-sales` reading Module 28's JSON events with a tolerant reader (replacing the seed's Module 27 line format), a dead-letter path, and a late topic for confirms after the grace; the publisher, the archiver `bookings-archiver` writing Parquet through DuckDB, the nightly load running `warehouse/fact_bookings.sql`, the reconciliation `sales-reconciliation`, a lag probe, and the row-store against column-store comparison on H2 and DuckDB.
- Changed `fact_bookings.sql`: the load day is bound as a parameter by the job instead of a fixed `SET VARIABLE` line.
- Added the broker setting `log.message.timestamp.after.max.ms` (30 days) to `compose.yaml`: the seeded on-sale is dated after the day the stack runs, and Kafka 4 refuses timestamps more than an hour ahead by default.
