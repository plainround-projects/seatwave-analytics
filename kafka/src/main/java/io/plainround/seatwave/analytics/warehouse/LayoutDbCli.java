package io.plainround.seatwave.analytics.warehouse;

import io.plainround.seatwave.analytics.BookingEvent;
import io.plainround.seatwave.analytics.Delivery;
import io.plainround.seatwave.analytics.OnSaleGenerator;
import io.plainround.seatwave.analytics.kafka.Options;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

/**
 * {@code LayoutCli}'s comparison on real embedded databases: the same {@code fact_bookings} rows of
 * the seeded on-sale in H2 (a row store: each row's columns together on its pages) and in DuckDB (a
 * column store), and the same query, confirmed tickets per show per minute, timed on each. Both run
 * in this JVM's memory, so the times compare layouts and engines, not disks.
 *
 * <p>Usage: {@code --confirms-per-second 1500 --runs 5}
 */
public final class LayoutDbCli {

  private static final String TABLE =
      """
      CREATE TABLE fact_bookings (booking_id UUID PRIMARY KEY, booking_version INTEGER NOT NULL,
        event_id BIGINT NOT NULL, customer_id BIGINT NOT NULL, seat_label VARCHAR NOT NULL,
        confirmed_at TIMESTAMP WITH TIME ZONE NOT NULL, status VARCHAR NOT NULL,
        status_changed_at TIMESTAMP WITH TIME ZONE NOT NULL, loaded_on DATE NOT NULL)
      """;

  // The same text runs on both engines.
  private static final String QUERY =
      """
      SELECT event_id, date_trunc('minute', confirmed_at) AS minute_start, count(*) AS confirmed
      FROM fact_bookings WHERE status = 'CONFIRMED'
      GROUP BY event_id, date_trunc('minute', confirmed_at)
      """;

  // H2 reuses the result of an identical query on an unchanged table; the timing must not.
  private static final String H2_URL = "jdbc:h2:mem:layout;OPTIMIZE_REUSE_RESULTS=FALSE";
  private static final int H2_BATCH = 10_000;
  private static final int MAX_RUNS = 50;
  private static final LocalDate LOADED_ON = LocalDate.of(2026, 11, 21);

  private record Row(BookingEvent confirm, BookingEvent latest) {}

  private LayoutDbCli() {}

  public static void main(String[] arguments) throws SQLException {
    Options options = Options.parse(arguments, Map.of("confirms-per-second", "1500", "runs", "5"));
    int confirmsPerSecond = options.integer("confirms-per-second", 1, 1_500);
    int runs = options.integer("runs", 1, MAX_RUNS);
    if (!options.problems().isEmpty()) {
      options.problems().forEach(System.err::println);
      System.exit(2);
    }
    List<Row> rows = rows(confirmsPerSecond);
    try (Connection h2 = DriverManager.getConnection(H2_URL);
        Connection duckDb = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement zone = duckDb.createStatement()) {
        zone.execute("SET TimeZone = 'UTC'");
      }
      long h2LoadNanos = timed(() -> loadH2(h2, rows));
      long duckLoadNanos = timed(() -> loadDuckDb(duckDb, rows));
      Map<String, Long> h2Answer = answer(h2);
      Map<String, Long> duckAnswer = answer(duckDb);
      long[] h2Nanos = runs(h2, runs);
      long[] duckNanos = runs(duckDb, runs);
      double h2Median = median(h2Nanos) / 1e6;
      double duckMedian = median(duckNanos) / 1e6;
      System.out.printf(
          Locale.ROOT,
          "fact_bookings: %,d rows; query: confirmed tickets per show per minute; %d runs each,"
              + " median%n",
          rows.size(),
          runs);
      System.out.printf(
          Locale.ROOT,
          "  H2 2.5.252 (row store, in memory):       load %.1f s, query %.1f ms%n",
          h2LoadNanos / 1e9,
          h2Median);
      System.out.printf(
          Locale.ROOT,
          "  DuckDB 2.0.0 (column store, in memory):  load %.1f s, query %.1f ms%n",
          duckLoadNanos / 1e9,
          duckMedian);
      System.out.printf(
          Locale.ROOT,
          "  the row store took %.1f times as long; same answer: %s (%,d windows, %,d confirmed)%n",
          h2Median / duckMedian,
          h2Answer.equals(duckAnswer),
          duckAnswer.size(),
          duckAnswer.values().stream().mapToLong(Long::longValue).sum());
    }
  }

  /** One row per booking: its confirm, and its newest event (the cancel, if any). */
  private static List<Row> rows(int confirmsPerSecond) {
    Map<java.util.UUID, Row> byBooking = new HashMap<>();
    List<Delivery> deliveries =
        new OnSaleGenerator(OnSaleGenerator.Settings.scaled(confirmsPerSecond)).generate();
    for (Delivery delivery : deliveries) {
      BookingEvent event = delivery.event();
      byBooking.merge(
          event.bookingId(),
          new Row(event.isConfirm() ? event : null, event),
          (known, next) ->
              new Row(
                  known.confirm() != null ? known.confirm() : next.confirm(),
                  next.latest().bookingVersion() > known.latest().bookingVersion()
                      ? next.latest()
                      : known.latest()));
    }
    return byBooking.values().stream().filter(row -> row.confirm() != null).toList();
  }

  private static void loadH2(Connection h2, List<Row> rows) throws SQLException {
    try (Statement statement = h2.createStatement()) {
      statement.execute(TABLE);
    }
    h2.setAutoCommit(false);
    try (PreparedStatement insert =
        h2.prepareStatement("INSERT INTO fact_bookings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
      int pending = 0;
      for (Row row : rows) {
        insert.setObject(1, row.confirm().bookingId());
        insert.setInt(2, row.latest().bookingVersion());
        insert.setLong(3, row.confirm().eventId());
        insert.setLong(4, row.confirm().customerId());
        insert.setString(5, row.confirm().seatLabel());
        insert.setObject(6, OffsetDateTime.ofInstant(row.confirm().occurredAt(), ZoneOffset.UTC));
        insert.setString(7, row.latest().isConfirm() ? "CONFIRMED" : "CANCELED");
        insert.setObject(8, OffsetDateTime.ofInstant(row.latest().occurredAt(), ZoneOffset.UTC));
        insert.setObject(9, LOADED_ON);
        insert.addBatch();
        if (++pending == H2_BATCH) {
          insert.executeBatch();
          pending = 0;
        }
      }
      insert.executeBatch();
    }
    h2.commit();
  }

  private static void loadDuckDb(Connection duckDb, List<Row> rows) throws SQLException {
    try (Statement statement = duckDb.createStatement()) {
      statement.execute(TABLE);
    }
    try (DuckDBAppender appender =
        duckDb
            .unwrap(DuckDBConnection.class)
            .createAppender(DuckDBConnection.DEFAULT_SCHEMA, "fact_bookings")) {
      for (Row row : rows) {
        appender.beginRow();
        appender.append(row.confirm().bookingId());
        appender.append(row.latest().bookingVersion());
        appender.append(row.confirm().eventId());
        appender.append(row.confirm().customerId());
        appender.append(row.confirm().seatLabel());
        appender.append(OffsetDateTime.ofInstant(row.confirm().occurredAt(), ZoneOffset.UTC));
        appender.append(row.latest().isConfirm() ? "CONFIRMED" : "CANCELED");
        appender.append(OffsetDateTime.ofInstant(row.latest().occurredAt(), ZoneOffset.UTC));
        appender.append(LOADED_ON);
        appender.endRow();
      }
    }
  }

  private static Map<String, Long> answer(Connection database) throws SQLException {
    Map<String, Long> confirmed = new TreeMap<>();
    try (Statement statement = database.createStatement();
        ResultSet rows = statement.executeQuery(QUERY)) {
      while (rows.next()) {
        Instant minute = rows.getObject(2, OffsetDateTime.class).toInstant();
        confirmed.put(rows.getLong(1) + "@" + minute, rows.getLong(3));
      }
    }
    return confirmed;
  }

  private static long[] runs(Connection database, int runs) throws SQLException {
    long[] nanos = new long[runs];
    for (int run = 0; run < runs; run++) {
      nanos[run] = timed(() -> answer(database));
    }
    return nanos;
  }

  private static double median(long[] nanos) {
    long[] sorted = nanos.clone();
    Arrays.sort(sorted);
    return sorted.length % 2 == 1
        ? sorted[sorted.length / 2]
        : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2.0;
  }

  private static long timed(SqlCall call) throws SQLException {
    long startedNanos = System.nanoTime();
    call.run();
    return System.nanoTime() - startedNanos;
  }

  @FunctionalInterface
  private interface SqlCall {
    void run() throws SQLException;
  }
}
