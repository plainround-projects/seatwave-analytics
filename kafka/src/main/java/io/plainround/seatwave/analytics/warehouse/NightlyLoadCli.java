package io.plainround.seatwave.analytics.warehouse;

import io.plainround.seatwave.analytics.kafka.Options;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;

/**
 * The job {@code nightly-bookings-load} (scheduled at 02:00): loads one UTC day of the lake into
 * {@code fact_bookings} of the DuckDB file and prints what it did and how long it took.
 *
 * <p>Usage: {@code --db build/warehouse/seatwave.duckdb --lake-parent . --day 2026-11-20}
 */
public final class NightlyLoadCli {

  private NightlyLoadCli() {}

  public static void main(String[] arguments) throws SQLException, IOException {
    Options options =
        Options.parse(
            arguments,
            Map.of(
                "db", "build/warehouse/seatwave.duckdb", "lake-parent", ".", "day", "2026-11-20"));
    Path database = options.path("db");
    Path lakeParent = options.path("lake-parent");
    var day = options.day("day");
    if (!Files.isDirectory(lakeParent.resolve("lake").resolve("bookings"))) {
      System.err.println(
          "no lake under " + lakeParent.toAbsolutePath() + ": run the archiver first");
      System.exit(2);
    }
    if (!options.problems().isEmpty()) {
      options.problems().forEach(System.err::println);
      System.exit(2);
    }
    Files.createDirectories(database.toAbsolutePath().getParent());
    long startedNanos = System.nanoTime();
    try (Warehouse warehouse = Warehouse.open(database, lakeParent)) {
      Warehouse.LoadReport report = warehouse.load(day);
      double seconds = (System.nanoTime() - startedNanos) / 1e9;
      System.out.printf(
          Locale.ROOT,
          "nightly-bookings-load %s: %,d records read, %,d rows inserted or updated, %,d rows in"
              + " fact_bookings; %.2f s%n",
          report.day(),
          report.recordsRead(),
          report.rowsChanged(),
          report.rowsAfter(),
          seconds);
    }
  }
}
