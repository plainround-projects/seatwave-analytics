package io.plainround.seatwave.analytics.warehouse;

import io.plainround.seatwave.analytics.BookingEvent;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

/**
 * The data lake's writer: appends booking events as Parquet files under {@code
 * <root>/bookings/date=YYYY-MM-DD/}, one new file per partition per call, partitioned by the UTC
 * day of {@code occurred_at}. DuckDB writes the files itself ({@code COPY ... PARTITION_BY ...
 * APPEND}), so the project needs no Parquet library. A call never rewrites an existing file, so a
 * repeated call (an archiver restarted before it committed its offsets) adds a second copy of the
 * same records, which the nightly load removes.
 *
 * <p>Not thread-safe: one archiver thread owns it. Close it once, at shutdown.
 */
public final class Lake implements AutoCloseable {

  private static final String STAGE =
      """
      CREATE TEMP TABLE stage (message_id VARCHAR, type VARCHAR, booking_id VARCHAR,
        booking_version INTEGER, customer_id BIGINT, event_id BIGINT, seat_label VARCHAR,
        occurred_at VARCHAR, date VARCHAR)
      """;

  private static final String COPY =
      """
      COPY (SELECT message_id::UUID AS message_id, type, booking_id::UUID AS booking_id,
              booking_version, customer_id, event_id, seat_label,
              occurred_at::TIMESTAMPTZ AS occurred_at, date::DATE AS date
            FROM stage)
      TO ? (FORMAT parquet, PARTITION_BY (date), APPEND, FILENAME_PATTERN 'part-{uuid}')
      """;

  private final Path bookingsFolder;
  private final Connection duckDb;

  private Lake(Path bookingsFolder, Connection duckDb) {
    this.bookingsFolder = bookingsFolder;
    this.duckDb = duckDb;
  }

  /** A writer for the lake under {@code root}; creates {@code root/bookings} if needed. */
  public static Lake open(Path root) throws SQLException, IOException {
    Path bookingsFolder = Objects.requireNonNull(root, "root").resolve("bookings");
    Files.createDirectories(bookingsFolder);
    Connection duckDb = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement statement = duckDb.createStatement()) {
      statement.execute("SET TimeZone = 'UTC'");
      statement.execute(STAGE);
    } catch (SQLException failed) {
      duckDb.close();
      throw failed;
    }
    return new Lake(bookingsFolder, duckDb);
  }

  /** Appends {@code events} as new Parquet files; returns how many records it wrote. */
  public int append(List<BookingEvent> events) throws SQLException {
    if (events.isEmpty()) {
      return 0;
    }
    try (DuckDBAppender appender =
        duckDb
            .unwrap(DuckDBConnection.class)
            .createAppender(DuckDBConnection.DEFAULT_SCHEMA, "stage")) {
      for (BookingEvent event : events) {
        appender.beginRow();
        appender.append(event.messageId().toString());
        appender.append(event.isConfirm() ? "BookingConfirmed" : "BookingCanceled");
        appender.append(event.bookingId().toString());
        appender.append(event.bookingVersion());
        appender.append(event.customerId());
        appender.append(event.eventId());
        appender.append(event.seatLabel());
        appender.append(event.occurredAt().toString());
        appender.append(event.occurredAt().atZone(ZoneOffset.UTC).toLocalDate().toString());
        appender.endRow();
      }
    }
    try (PreparedStatement copy = duckDb.prepareStatement(COPY);
        Statement clear = duckDb.createStatement()) {
      copy.setString(1, bookingsFolder.toAbsolutePath().toString());
      copy.execute();
      clear.execute("DELETE FROM stage");
    }
    return events.size();
  }

  @Override
  public void close() throws SQLException {
    duckDb.close();
  }
}
