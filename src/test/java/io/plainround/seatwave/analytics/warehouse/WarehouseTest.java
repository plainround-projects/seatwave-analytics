package io.plainround.seatwave.analytics.warehouse;

import static org.assertj.core.api.Assertions.assertThat;

import io.plainround.seatwave.analytics.BookingEvent;
import io.plainround.seatwave.analytics.Delivery;
import io.plainround.seatwave.analytics.OnSaleGenerator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The lake and the nightly load on a real embedded DuckDB: Parquet files by day, a load that keeps
 * one row per booking from an archive written at least once, and a second load that changes no row.
 */
class WarehouseTest {

  private static final LocalDate SALE_DAY = LocalDate.of(2026, 11, 20);
  private static final int SMALL_ON_SALE_RATE = 10; // confirms a second: 9,000 in the 15 minutes

  private static List<BookingEvent> events;

  @TempDir Path folder;

  @BeforeAll
  static void generate() {
    events =
        new OnSaleGenerator(OnSaleGenerator.Settings.scaled(SMALL_ON_SALE_RATE))
            .generate().stream()
                .filter(delivery -> !delivery.isRedelivery())
                .map(Delivery::event)
                .toList();
  }

  @Test
  void writesOneParquetFilePerDayAndCallUnderThePartitionFolder() throws Exception {
    try (Lake lake = Lake.open(folder.resolve("lake"))) {
      lake.append(events);
      lake.append(events.subList(0, 1));
    }

    try (var files = Files.list(folder.resolve("lake/bookings/date=2026-11-20"))) {
      assertThat(files.filter(file -> file.toString().endsWith(".parquet")).count()).isEqualTo(2);
    }
  }

  @Test
  void loadsOneRowPerBookingFromAnArchiveWrittenTwice() throws Exception {
    archiveTwice();

    try (Warehouse warehouse = Warehouse.open(folder.resolve("w.duckdb"), folder)) {
      Warehouse.LoadReport report = warehouse.load(SALE_DAY);

      assertThat(report.recordsRead()).isEqualTo(2L * events.size());
      assertThat(report.rowsAfter()).isEqualTo(confirmCount());
      assertThat(warehouse.confirmsPerMinute()).isEqualTo(truth());
    }
  }

  @Test
  void loadingTheSameDayAgainChangesNoRow() throws Exception {
    archiveTwice();

    try (Warehouse warehouse = Warehouse.open(folder.resolve("w.duckdb"), folder)) {
      warehouse.load(SALE_DAY);
      Warehouse.LoadReport second = warehouse.load(SALE_DAY);

      assertThat(second.rowsChanged()).isZero();
      assertThat(second.rowsAfter()).isEqualTo(confirmCount());
    }
  }

  @Test
  void loadsNothingForADayTheLakeDoesNotHold() throws Exception {
    archiveTwice();

    try (Warehouse warehouse = Warehouse.open(folder.resolve("w.duckdb"), folder)) {
      Warehouse.LoadReport report = warehouse.load(SALE_DAY.plusDays(1));

      assertThat(report.recordsRead()).isZero();
      assertThat(report.rowsAfter()).isZero();
    }
  }

  private void archiveTwice() throws Exception {
    // An archiver that crashed after writing but before committing writes the same records again.
    try (Lake lake = Lake.open(folder.resolve("lake"))) {
      lake.append(events);
      lake.append(events);
    }
  }

  private static long confirmCount() {
    return events.stream().filter(BookingEvent::isConfirm).count();
  }

  private static Map<String, Long> truth() {
    return events.stream()
        .filter(BookingEvent::isConfirm)
        .collect(
            Collectors.groupingBy(
                event ->
                    event.eventId()
                        + "@"
                        + Instant.ofEpochSecond(event.occurredAt().getEpochSecond() / 60 * 60),
                TreeMap::new,
                Collectors.counting()));
  }
}
