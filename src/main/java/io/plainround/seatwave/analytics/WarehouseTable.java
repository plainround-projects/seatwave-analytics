package io.plainround.seatwave.analytics;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * An in-memory stand-in for the warehouse table {@code fact_bookings}: one row per booking, keyed
 * by {@code booking_id}, written only by idempotent upserts in which a newer {@code
 * booking_version} wins and an equal or older one changes nothing. Section 8 shows the same rule in
 * SQL.
 *
 * <p>Thread-safety: not thread-safe; the nightly batch is its only writer.
 */
final class WarehouseTable {

  /** The row's state. */
  enum Status {
    CONFIRMED,
    CANCELED
  }

  /** What one upsert did. */
  enum UpsertOutcome {
    INSERTED,
    UPDATED,
    IGNORED_SAME_VERSION,
    IGNORED_OLDER_VERSION,
    IGNORED_NO_CONFIRM
  }

  /**
   * One row of {@code fact_bookings}.
   *
   * @param bookingId the primary key
   * @param bookingVersion the version of the last event applied
   * @param eventId the show
   * @param customerId the customer (an id only)
   * @param seatLabel the seat
   * @param confirmedAt event time of the confirm: the minute finance reports sales in
   * @param status confirmed or canceled
   * @param statusChangedAt event time of the last change
   */
  record FactBooking(
      UUID bookingId,
      int bookingVersion,
      long eventId,
      long customerId,
      String seatLabel,
      Instant confirmedAt,
      Status status,
      Instant statusChangedAt) {

    FactBooking {
      Objects.requireNonNull(bookingId, "bookingId");
      Objects.requireNonNull(seatLabel, "seatLabel");
      Objects.requireNonNull(confirmedAt, "confirmedAt");
      Objects.requireNonNull(status, "status");
      Objects.requireNonNull(statusChangedAt, "statusChangedAt");
    }
  }

  private final Map<UUID, FactBooking> rowsByBookingId = new HashMap<>();

  /** Applies one event; the outcome says whether the table changed. */
  UpsertOutcome upsert(BookingEvent event) {
    FactBooking existing = rowsByBookingId.get(event.bookingId());
    if (existing == null) {
      if (!event.isConfirm()) {
        return UpsertOutcome.IGNORED_NO_CONFIRM; // a cancel whose confirm is not in the table
      }
      rowsByBookingId.put(event.bookingId(), rowFor(event, event.occurredAt()));
      return UpsertOutcome.INSERTED;
    }
    if (event.bookingVersion() == existing.bookingVersion()) {
      return UpsertOutcome.IGNORED_SAME_VERSION; // a duplicate: the row already holds this fact
    }
    if (event.bookingVersion() < existing.bookingVersion()) {
      return UpsertOutcome.IGNORED_OLDER_VERSION;
    }
    rowsByBookingId.put(event.bookingId(), rowFor(event, existing.confirmedAt()));
    return UpsertOutcome.UPDATED;
  }

  int rowCount() {
    return rowsByBookingId.size();
  }

  Collection<FactBooking> rows() {
    return List.copyOf(rowsByBookingId.values());
  }

  /**
   * Confirms per show per minute of {@code confirmed_at} (every booking, canceled or not), and
   * cancels per minute of the cancel: the same two measures the stream counts.
   */
  Map<MinuteWindows.WindowKey, MinuteWindows.Totals> perMinute(Duration windowSize) {
    Map<MinuteWindows.WindowKey, MinuteWindows.Totals> totals = new HashMap<>();
    for (FactBooking row : rowsByBookingId.values()) {
      totals.merge(
          keyOf(row.eventId(), row.confirmedAt(), windowSize),
          new MinuteWindows.Totals(1, 0),
          MinuteWindows.Totals::plus);
      if (row.status() == Status.CANCELED) {
        totals.merge(
            keyOf(row.eventId(), row.statusChangedAt(), windowSize),
            new MinuteWindows.Totals(0, 1),
            MinuteWindows.Totals::plus);
      }
    }
    return totals;
  }

  /** Bookings still confirmed at the end of the load, by the minute they were confirmed. */
  Map<Instant, Long> stillConfirmedByMinute(Duration windowSize) {
    Map<Instant, Long> net = new HashMap<>();
    for (FactBooking row : rowsByBookingId.values()) {
      if (row.status() == Status.CONFIRMED) {
        net.merge(keyOf(row.eventId(), row.confirmedAt(), windowSize).windowStart(), 1L, Long::sum);
      }
    }
    return net;
  }

  private static FactBooking rowFor(BookingEvent event, Instant confirmedAt) {
    Status status = event.isConfirm() ? Status.CONFIRMED : Status.CANCELED;
    return new FactBooking(
        event.bookingId(),
        event.bookingVersion(),
        event.eventId(),
        event.customerId(),
        event.seatLabel(),
        confirmedAt,
        status,
        event.occurredAt());
  }

  private static MinuteWindows.WindowKey keyOf(long eventId, Instant time, Duration windowSize) {
    long sizeMillis = windowSize.toMillis();
    Instant start =
        Instant.ofEpochMilli(Math.floorDiv(time.toEpochMilli(), sizeMillis) * sizeMillis);
    return new MinuteWindows.WindowKey(eventId, start);
  }
}
