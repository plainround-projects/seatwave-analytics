package io.plainround.seatwave.analytics.warehouse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The warehouse: an embedded DuckDB database file holding {@code fact_bookings} and the view {@code
 * sales_per_minute}, both defined by {@code warehouse/fact_bookings.sql}. {@link #load} is the job
 * {@code nightly-bookings-load} for one day: it runs that file's statements with the day bound as a
 * parameter. Loading a day twice changes no row.
 *
 * <p>Not thread-safe: one job owns it. Close it once.
 */
public final class Warehouse implements AutoCloseable {

  /** The file on the class path (the project's {@code warehouse/} folder). */
  static final String SQL_RESOURCE = "/fact_bookings.sql";

  // A statement ends with ';' at the end of a line, optionally followed by a comment.
  private static final String STATEMENT_END = ";[ \\t]*(--[^\\n]*)?\\n";

  private final Connection duckDb;
  private final List<String> statements;

  private Warehouse(Connection duckDb, List<String> statements) {
    this.duckDb = duckDb;
    this.statements = statements;
  }

  /**
   * What one load did.
   *
   * @param day the UTC day loaded
   * @param recordsRead archive records of that day, repeats included
   * @param rowsChanged rows inserted or updated (0 on a second load of the same day)
   * @param rowsAfter rows in {@code fact_bookings} afterwards
   */
  public record LoadReport(LocalDate day, long recordsRead, long rowsChanged, long rowsAfter) {
    public LoadReport {
      Objects.requireNonNull(day, "day");
    }
  }

  /**
   * Opens (or creates) the database {@code file}, whose relative lake paths resolve under {@code
   * lakeParent} (the folder that holds {@code lake/}).
   */
  public static Warehouse open(Path file, Path lakeParent) throws SQLException, IOException {
    Objects.requireNonNull(file, "file");
    Objects.requireNonNull(lakeParent, "lakeParent");
    List<String> statements = readStatements();
    Connection duckDb = DriverManager.getConnection("jdbc:duckdb:" + file.toAbsolutePath());
    try (Statement settings = duckDb.createStatement();
        PreparedStatement searchPath = duckDb.prepareStatement("SET file_search_path = ?")) {
      settings.execute("SET TimeZone = 'UTC'");
      searchPath.setString(1, lakeParent.toAbsolutePath().toString());
      searchPath.execute();
      settings.execute(statements.get(0)); // CREATE TABLE IF NOT EXISTS fact_bookings
    } catch (SQLException failed) {
      duckDb.close();
      throw failed;
    }
    return new Warehouse(duckDb, statements);
  }

  /** Loads {@code day} from the lake; the statements run in the file's order. */
  public LoadReport load(LocalDate day) throws SQLException {
    Objects.requireNonNull(day, "day");
    try (PreparedStatement setDay = duckDb.prepareStatement("SET VARIABLE load_day = ?::DATE");
        Statement statement = duckDb.createStatement()) {
      setDay.setString(1, day.toString());
      setDay.execute();
      long recordsRead =
          single(
              statement,
              "SELECT count(*) FROM read_parquet('lake/bookings/*/*.parquet',"
                  + " hive_partitioning = true) WHERE date = getvariable('load_day')");
      long rowsChanged = statement.executeUpdate(statements.get(1)); // the upsert
      statement.execute(statements.get(2)); // the view
      return new LoadReport(
          day, recordsRead, rowsChanged, single(statement, "SELECT count(*) FROM fact_bookings"));
    }
  }

  /**
   * The view {@code sales_per_minute} as the reconciliation reads it: confirms per window key
   * {@code <event_id>@<minute start>}, in key order.
   */
  public Map<String, Long> confirmsPerMinute() throws SQLException {
    Map<String, Long> confirms = new TreeMap<>();
    try (Statement statement = duckDb.createStatement();
        ResultSet rows =
            statement.executeQuery("SELECT event_id, minute, confirms FROM sales_per_minute")) {
      while (rows.next()) {
        OffsetDateTime minute = rows.getObject(2, OffsetDateTime.class);
        confirms.put(rows.getLong(1) + "@" + minute.toInstant(), rows.getLong(3));
      }
    }
    return confirms;
  }

  @Override
  public void close() throws SQLException {
    duckDb.close();
  }

  private static long single(Statement statement, String query) throws SQLException {
    try (ResultSet result = statement.executeQuery(query)) {
      result.next();
      return result.getLong(1);
    }
  }

  private static List<String> readStatements() throws IOException {
    try (InputStream sql = Warehouse.class.getResourceAsStream(SQL_RESOURCE)) {
      if (sql == null) {
        throw new IOException(SQL_RESOURCE + " is not on the class path");
      }
      List<String> statements =
          Arrays.stream(new String(sql.readAllBytes(), StandardCharsets.UTF_8).split(STATEMENT_END))
              .map(String::strip)
              .filter(text -> !text.isEmpty())
              .toList();
      if (statements.size() != 3) {
        throw new IOException(
            SQL_RESOURCE + " must hold 3 statements (table, load, view): " + statements.size());
      }
      return statements;
    }
  }
}
