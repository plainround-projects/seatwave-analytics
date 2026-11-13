package io.plainround.seatwave.analytics;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The nightly job {@code nightly-bookings-load}: reads one UTC day of booking events from the
 * archive (the raw records the lake holds, duplicates and late arrivals included) and upserts them
 * into {@code fact_bookings}. Running it twice leaves the table as running it once.
 *
 * <p>Thread-safety: stateless; safe to share.
 */
final class NightlyBatch {

  /**
   * What one load did.
   *
   * @param day the UTC day loaded
   * @param recordsRead archive records whose event time falls on that day
   * @param outcomes how many records led to each upsert outcome
   * @param rowsAfter rows in the table after the load
   */
  record LoadSummary(
      LocalDate day,
      long recordsRead,
      Map<WarehouseTable.UpsertOutcome, Long> outcomes,
      int rowsAfter) {

    LoadSummary {
      Objects.requireNonNull(day, "day");
      outcomes = Map.copyOf(outcomes);
    }

    long count(WarehouseTable.UpsertOutcome outcome) {
      return outcomes.getOrDefault(outcome, 0L);
    }
  }

  /** Loads {@code day} from {@code archive} into {@code table}. */
  LoadSummary load(LocalDate day, List<Delivery> archive, WarehouseTable table) {
    Objects.requireNonNull(day, "day");
    Objects.requireNonNull(archive, "archive");
    Objects.requireNonNull(table, "table");
    Instant dayStart = day.atStartOfDay(ZoneOffset.UTC).toInstant();
    Instant dayEnd = day.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant();
    // The batch sees the whole day at once, so it can order by booking and version, which no
    // stream can: every cancel is applied after its confirm, whatever order they arrived in.
    List<BookingEvent> dayEvents =
        archive.stream()
            .map(Delivery::event)
            .filter(event ->
                !event.occurredAt().isBefore(dayStart) && event.occurredAt().isBefore(dayEnd))
            .sorted(
                Comparator.comparing(BookingEvent::bookingId)
                    .thenComparingInt(BookingEvent::bookingVersion))
            .toList();
    Map<WarehouseTable.UpsertOutcome, Long> outcomes =
        new EnumMap<>(WarehouseTable.UpsertOutcome.class);
    for (BookingEvent event : dayEvents) {
      outcomes.merge(table.upsert(event), 1L, Long::sum);
    }
    return new LoadSummary(day, dayEvents.size(), outcomes, table.rowCount());
  }
}
