package io.plainround.seatwave.analytics.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.plainround.seatwave.analytics.BookingEvent;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Module 28's tolerant reader: what it accepts, what it ignores and what it refuses, and why. */
class BookingEventJsonTest {

  private static final BookingEvent CANCEL =
      new BookingEvent(
          UUID.fromString("00000000-0000-4000-8000-000000000001"),
          BookingEvent.Type.BOOKING_CANCELED,
          UUID.fromString("00000000-0000-7000-8000-000000000002"),
          2,
          1_000,
          7,
          "B-12",
          Instant.parse("2026-11-20T19:20:00Z"));

  @Test
  void readsBackWhatItWrites() {
    assertThat(BookingEventJson.read(BookingEventJson.write(CANCEL)))
        .isEqualTo(new BookingEventJson.ReadResult.Readable(CANCEL));
  }

  @Test
  void ignoresAFieldItDoesNotKnow() {
    String withPrice = BookingEventJson.write(CANCEL).replace("}", ",\"price\":\"80.00\"}");

    assertThat(BookingEventJson.read(withPrice))
        .isEqualTo(new BookingEventJson.ReadResult.Readable(CANCEL));
  }

  @Test
  void namesTheRequiredFieldThatIsMissing() {
    String withoutTime = BookingEventJson.write(CANCEL).replace("\"occurred_at\"", "\"when\"");

    assertThat(reason(withoutTime)).isEqualTo("missing required fields [occurred_at]");
  }

  @Test
  void refusesASchemaVersionItWasNotBuiltFor() {
    String version3 =
        BookingEventJson.write(CANCEL).replace("\"schema_version\":1", "\"schema_version\":3");

    assertThat(reason(version3)).isEqualTo("schema_version 3 unknown");
  }

  @Test
  void refusesATypeThatIsNotABookingEvent() {
    String seatLocked = BookingEventJson.write(CANCEL).replace("BookingCanceled", "SeatLocked");

    assertThat(reason(seatLocked)).isEqualTo("type \"SeatLocked\" is not a booking event");
  }

  @Test
  void refusesFieldsOfTheWrongForm() {
    assertThat(reason(BookingEventJson.write(CANCEL).replace("B-12", "row 12")))
        .startsWith("bad field: seatLabel");
    assertThat(
            reason(BookingEventJson.write(CANCEL).replace("\"event_id\":7", "\"event_id\":\"x\"")))
        .isEqualTo("bad field: event_id is not a whole number: \"x\"");
    assertThat(reason(BookingEventJson.write(CANCEL).replace("2026-11-20T19:20:00Z", "tonight")))
        .startsWith("bad field: Text 'tonight'");
  }

  @Test
  void refusesValuesThatAreNotOneJsonObject() {
    assertThat(reason(null)).isEqualTo("no value (a tombstone)");
    assertThat(reason("[1, 2]")).isEqualTo("not a JSON object");
    assertThat(reason("{\"type\":")).startsWith("not JSON");
  }

  private static String reason(String value) {
    return switch (BookingEventJson.read(value)) {
      case BookingEventJson.ReadResult.Unreadable unreadable -> unreadable.reason();
      case BookingEventJson.ReadResult.Readable readable -> "readable: " + readable.event();
    };
  }
}
