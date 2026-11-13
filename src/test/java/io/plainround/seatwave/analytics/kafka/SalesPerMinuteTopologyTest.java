package io.plainround.seatwave.analytics.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.plainround.seatwave.analytics.BookingEvent;
import io.plainround.seatwave.analytics.Delivery;
import io.plainround.seatwave.analytics.OnSaleGenerator;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The real topology on Kafka Streams' test driver (no broker): event-time windows, the late topic
 * instead of a silent drop, the dead-letter path, and the invariant that every confirm is counted
 * once or kept once in the late topic.
 */
class SalesPerMinuteTopologyTest {

  private static final Instant MINUTE_1907 = Instant.parse("2026-11-20T19:07:00Z");
  private static final int SMALL_ON_SALE_RATE = 10; // confirms a second: 9,000 in the 15 minutes

  @TempDir Path stateFolder;

  private TopologyTestDriver driver;
  private TestInputTopic<String, String> bookings;
  private TestOutputTopic<String, Long> salesPerMinute;
  private TestOutputTopic<String, String> late;
  private TestOutputTopic<String, String> deadLetters;

  @BeforeEach
  void startDriver() {
    driver =
        new TopologyTestDriver(
            SalesPerMinuteTopology.build(),
            SalesPerMinuteTopology.settings("unused:9092", stateFolder));
    bookings =
        driver.createInputTopic(
            SalesPerMinuteTopology.SOURCE_TOPIC, new StringSerializer(), new StringSerializer());
    salesPerMinute =
        driver.createOutputTopic(
            SalesPerMinuteTopology.OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());
    late =
        driver.createOutputTopic(
            SalesPerMinuteTopology.LATE_TOPIC, new StringDeserializer(), new StringDeserializer());
    deadLetters =
        driver.createOutputTopic(
            SalesPerMinuteTopology.DEAD_LETTER_TOPIC,
            new StringDeserializer(),
            new StringDeserializer());
  }

  @AfterEach
  void closeDriver() {
    driver.close();
  }

  @Test
  void countsAConfirmInTheMinuteItOccurredNotTheMinuteItWasSent() {
    BookingEvent confirm = confirm(7, MINUTE_1907.plusSeconds(30));

    // Sent four minutes after it occurred: a processing-time window would put it in 19:11.
    send(confirm, MINUTE_1907.plusSeconds(240));

    assertThat(salesPerMinute.readKeyValuesToMap())
        .containsExactly(Map.entry(SalesPerMinuteTopology.windowKey(7, MINUTE_1907), 1L));
  }

  @Test
  void sendsAConfirmAfterTheGraceToTheLateTopicKeyedByItsWindow() {
    BookingEvent onTime = confirm(7, MINUTE_1907.plusSeconds(10));
    BookingEvent later = confirm(7, MINUTE_1907.plusSeconds(200)); // stream time 19:10:20
    BookingEvent tooLate = confirm(7, MINUTE_1907.plusSeconds(20)); // 19:07 closed at 19:10:10

    send(onTime, onTime.occurredAt());
    send(later, later.occurredAt());
    send(tooLate, later.occurredAt());

    assertThat(late.readKeyValuesToList())
        .singleElement()
        .satisfies(record -> {
          assertThat(record.key).isEqualTo(SalesPerMinuteTopology.windowKey(7, MINUTE_1907));
          assertThat(record.value).contains(tooLate.messageId().toString());
        });
    assertThat(salesPerMinute.readKeyValuesToMap())
        .containsEntry(SalesPerMinuteTopology.windowKey(7, MINUTE_1907), 1L);
  }

  @Test
  void parksAnUnreadableRecordInTheDeadLetterTopicAndKeepsCounting() {
    BookingEvent confirm = confirm(7, MINUTE_1907.plusSeconds(5));

    bookings.pipeInput("unparseable-1", "{\"type\":\"BookingConfirmed\",", MINUTE_1907);
    send(confirm, confirm.occurredAt());

    TestRecord<String, String> parked = deadLetters.readRecord();
    assertThat(parked.key()).isEqualTo("unparseable-1");
    assertThat(parked.value()).isEqualTo("{\"type\":\"BookingConfirmed\",");
    assertThat(header(parked, "error")).startsWith("not JSON");
    assertThat(header(parked, "consumer-group")).isEqualTo("analytics-sales");
    assertThat(header(parked, "source")).isEqualTo("seatwave.bookings/0/0");
    assertThat(salesPerMinute.readKeyValuesToMap())
        .containsEntry(SalesPerMinuteTopology.windowKey(7, MINUTE_1907), 1L);
  }

  @Test
  void countsEveryConfirmOnceOrKeepsItOnceInTheLateTopic() {
    List<Delivery> appendOrder =
        new OnSaleGenerator(OnSaleGenerator.Settings.scaled(SMALL_ON_SALE_RATE))
            .generate().stream()
                .filter(delivery -> !delivery.isRedelivery())
                .sorted(
                    Comparator.comparing(Delivery::appendedAt)
                        .thenComparing(delivery -> delivery.event().bookingId()))
                .toList();
    Map<String, Long> truth =
        appendOrder.stream()
            .map(Delivery::event)
            .filter(BookingEvent::isConfirm)
            .collect(
                Collectors.groupingBy(
                    SalesPerMinuteTopologyTest::windowKey, TreeMap::new, Collectors.counting()));

    appendOrder.forEach(delivery -> send(delivery.event(), delivery.appendedAt()));
    Map<String, Long> counted = new TreeMap<>(salesPerMinute.readKeyValuesToMap());
    Map<String, Long> kept =
        late.readKeyValuesToList().stream()
            .collect(Collectors.groupingBy(record -> record.key, Collectors.counting()));
    Map<String, Long> countedPlusKept = new TreeMap<>(counted);
    kept.forEach((key, confirms) -> countedPlusKept.merge(key, confirms, Long::sum));

    assertThat(kept).as("the stalled shard must leave confirms after the grace").isNotEmpty();
    assertThat(countedPlusKept).isEqualTo(truth);
  }

  private void send(BookingEvent event, Instant sentAt) {
    bookings.pipeInput(event.bookingId().toString(), BookingEventJson.write(event), sentAt);
  }

  private static String windowKey(BookingEvent event) {
    Instant occurredAt = event.occurredAt();
    long windowMillis = SalesPerMinuteTopology.WINDOW.toMillis();
    long startMillis = occurredAt.toEpochMilli() - occurredAt.toEpochMilli() % windowMillis;
    return SalesPerMinuteTopology.windowKey(event.eventId(), Instant.ofEpochMilli(startMillis));
  }

  private static String header(TestRecord<String, String> record, String name) {
    Header header = record.headers().lastHeader(name);
    assertThat(header).as("header " + name).isNotNull();
    return new String(header.value(), StandardCharsets.UTF_8);
  }

  private static BookingEvent confirm(long eventId, Instant occurredAt) {
    return new BookingEvent(
        UUID.nameUUIDFromBytes(("message-" + occurredAt).getBytes(StandardCharsets.UTF_8)),
        BookingEvent.Type.BOOKING_CONFIRMED,
        UUID.nameUUIDFromBytes(("booking-" + occurredAt).getBytes(StandardCharsets.UTF_8)),
        1,
        1_000,
        eventId,
        "A-7",
        occurredAt);
  }
}
