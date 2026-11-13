package io.plainround.seatwave.analytics.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.plainround.seatwave.analytics.BookingEvent;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

/**
 * The booking events of {@code seatwave.bookings} in Module 28's form: one JSON object per record,
 * the payload's fields plus {@code type}, {@code schema_version}, {@code aggregate_version} and
 * {@code message_id}. The reader is Module 28's tolerant reader: it requires only the fields
 * analytics uses, ignores any field it does not know (so a newer optional field breaks nothing),
 * and accepts the schema versions it was built for.
 *
 * <p>Thread-safety: stateless apart from Jackson's mapper, which is thread-safe once configured.
 */
public final class BookingEventJson {

  /**
   * The fields this reader requires, as Module 28's {@code EventSchema.missingFrom} checks them.
   */
  static final List<String> REQUIRED =
      List.of(
          "type",
          "schema_version",
          "message_id",
          "booking_id",
          "booking_version",
          "customer_id",
          "event_id",
          "seat_label",
          "occurred_at");

  /** Module 28 wrote versions 1 and 2 (2 added optional price and currency); both read alike. */
  static final Set<Integer> READABLE_VERSIONS = Set.of(1, 2);

  private static final int WRITTEN_VERSION = 1;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Map<String, BookingEvent.Type> TYPES =
      Map.of(
          "BookingConfirmed", BookingEvent.Type.BOOKING_CONFIRMED,
          "BookingCanceled", BookingEvent.Type.BOOKING_CANCELED);

  /** What one record turned out to be. */
  public sealed interface ReadResult {

    /** A record this reader can use. */
    record Readable(BookingEvent event) implements ReadResult {
      public Readable {
        Objects.requireNonNull(event, "event");
      }
    }

    /** A record it cannot use, and why: it goes to the dead-letter topic untouched. */
    record Unreadable(String reason) implements ReadResult {
      public Unreadable {
        Objects.requireNonNull(reason, "reason");
      }
    }
  }

  private BookingEventJson() {}

  /** The JSON object Module 28's relay would publish for {@code event}. */
  public static String write(BookingEvent event) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("booking_id", event.bookingId().toString());
    node.put("booking_version", event.bookingVersion());
    node.put("customer_id", event.customerId());
    node.put("event_id", event.eventId());
    node.put("seat_label", event.seatLabel());
    node.put("occurred_at", event.occurredAt().toString());
    node.put("type", event.isConfirm() ? "BookingConfirmed" : "BookingCanceled");
    node.put("schema_version", WRITTEN_VERSION);
    node.put("aggregate_version", event.bookingVersion());
    node.put("message_id", event.messageId().toString());
    return node.toString();
  }

  /**
   * Reads one record's value. Never throws for bad input: a missing value, text that is not JSON, a
   * missing required field, an unknown type or version, or a field of the wrong form each come back
   * as {@link ReadResult.Unreadable} with the reason.
   */
  public static ReadResult read(String value) {
    if (value == null) {
      return new ReadResult.Unreadable("no value (a tombstone)");
    }
    JsonNode node;
    try {
      node = MAPPER.readTree(value);
    } catch (JsonProcessingException notJson) {
      return new ReadResult.Unreadable("not JSON: " + notJson.getOriginalMessage());
    }
    if (node == null || !node.isObject()) {
      return new ReadResult.Unreadable("not a JSON object");
    }
    List<String> missing = REQUIRED.stream().filter(field -> !node.hasNonNull(field)).toList();
    if (!missing.isEmpty()) {
      return new ReadResult.Unreadable("missing required fields " + missing);
    }
    int schemaVersion = node.get("schema_version").asInt(-1);
    if (!READABLE_VERSIONS.contains(schemaVersion)) {
      return new ReadResult.Unreadable("schema_version " + node.get("schema_version") + " unknown");
    }
    BookingEvent.Type type = TYPES.get(node.get("type").asText());
    if (type == null) {
      return new ReadResult.Unreadable("type " + node.get("type") + " is not a booking event");
    }
    return toEvent(node, type);
  }

  /** Reads only {@code occurred_at}, for the timestamp extractor; empty when it cannot. */
  static OptionalLong occurredAtMillis(String value) {
    return switch (read(value)) {
      case ReadResult.Readable readable ->
          OptionalLong.of(readable.event().occurredAt().toEpochMilli());
      case ReadResult.Unreadable unreadable -> OptionalLong.empty();
    };
  }

  private static ReadResult toEvent(JsonNode node, BookingEvent.Type type) {
    try {
      return new ReadResult.Readable(
          new BookingEvent(
              UUID.fromString(node.get("message_id").asText()),
              type,
              UUID.fromString(node.get("booking_id").asText()),
              integer(node, "booking_version"),
              whole(node, "customer_id"),
              whole(node, "event_id"),
              node.get("seat_label").asText(),
              Instant.parse(node.get("occurred_at").asText())));
    } catch (IllegalArgumentException | DateTimeParseException badField) {
      // UUID.fromString and the event's compact constructor reject with IllegalArgumentException.
      return new ReadResult.Unreadable("bad field: " + badField.getMessage());
    }
  }

  private static int integer(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (!value.canConvertToInt()) {
      throw new IllegalArgumentException(field + " is not a whole number: " + value);
    }
    return value.intValue();
  }

  private static long whole(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (!value.canConvertToLong()) {
      throw new IllegalArgumentException(field + " is not a whole number: " + value);
    }
    return value.longValue();
  }
}
