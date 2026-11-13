package io.plainround.seatwave.analytics;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * One record as the stream job {@code analytics-sales} receives it: the event, where it sits in the
 * log, when the broker appended it, and when the job read it (processing time).
 *
 * @param event the booking event; its {@code occurredAt} is the event time
 * @param partition the partition of {@code seatwave.bookings}, 0 to 23
 * @param offset the log offset in that partition (Module 27)
 * @param appendedAt when the broker stored it
 * @param processedAt when the job read it: processing time
 * @param isRedelivery true for a second copy of a record already read, after a rebalance
 */
public record Delivery(
    BookingEvent event,
    int partition,
    long offset,
    Instant appendedAt,
    Instant processedAt,
    boolean isRedelivery) {

  public Delivery {
    Objects.requireNonNull(event, "event");
    Objects.requireNonNull(appendedAt, "appendedAt");
    Objects.requireNonNull(processedAt, "processedAt");
    if (partition < 0 || offset < 0) {
      throw new IllegalArgumentException("partition and offset must not be negative");
    }
    if (appendedAt.isBefore(event.occurredAt()) || processedAt.isBefore(appendedAt)) {
      throw new IllegalArgumentException("times must run occurred <= appended <= processed");
    }
  }

  /** How long after its event time the job saw this record. */
  Duration delay() {
    return Duration.between(event.occurredAt(), processedAt);
  }
}
