package io.plainround.seatwave.analytics;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * One fact from the topic {@code seatwave.bookings} (Module 27), with the fields analytics needs.
 * {@code occurredAt} is the event time: when the booking's change committed on its shard. It
 * carries the customer's id, never a name or an email address, so analytics holds no contact data.
 *
 * @param messageId unique per message; a redelivery of the same record keeps it
 * @param type what happened
 * @param bookingId the booking, the record key on the topic
 * @param bookingVersion 1 when confirmed, 2 when canceled
 * @param customerId who booked (an id only)
 * @param eventId the show
 * @param seatLabel the seat, such as {@code A-7}
 * @param occurredAt event time, UTC
 */
public record BookingEvent(
    UUID messageId,
    Type type,
    UUID bookingId,
    int bookingVersion,
    long customerId,
    long eventId,
    String seatLabel,
    Instant occurredAt) {

  private static final Pattern SEAT_LABEL = Pattern.compile("[A-Z]{1,2}-[0-9]{1,4}");

  /** The two event types of Module 27 (US spelling: {@code BookingCanceled}). */
  public enum Type {
    BOOKING_CONFIRMED,
    BOOKING_CANCELED
  }

  public BookingEvent {
    Objects.requireNonNull(messageId, "messageId");
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(bookingId, "bookingId");
    Objects.requireNonNull(seatLabel, "seatLabel");
    Objects.requireNonNull(occurredAt, "occurredAt");
    if (bookingVersion < 1) {
      throw new IllegalArgumentException("bookingVersion must be 1 or more: " + bookingVersion);
    }
    if (customerId < 1 || eventId < 1) {
      throw new IllegalArgumentException("customerId and eventId must be positive");
    }
    if (!SEAT_LABEL.matcher(seatLabel).matches()) {
      throw new IllegalArgumentException("seatLabel is not like A-7: " + seatLabel);
    }
  }

  public boolean isConfirm() {
    return type == Type.BOOKING_CONFIRMED;
  }
}
