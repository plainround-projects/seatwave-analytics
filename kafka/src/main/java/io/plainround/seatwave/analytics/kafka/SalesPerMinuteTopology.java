package io.plainround.seatwave.analytics.kafka;

import io.plainround.seatwave.analytics.BookingEvent;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

/**
 * The stream job {@code analytics-sales} on Kafka Streams: confirms per show per minute of event
 * time ({@code occurred_at}), updated as records arrive, written to {@code
 * seatwave.analytics.sales-per-minute} for the dashboard, keyed {@code <event_id>@<minute start>}.
 *
 * <p>Two kinds of record leave the count on purpose, and neither is lost. A record the reader
 * cannot use goes, untouched, to {@code seatwave.bookings.dlq} with Module 27's dead-letter
 * headers, and the job carries on. A confirm that arrives after its window's grace goes to {@code
 * seatwave.analytics.late-bookings}, keyed by the window it belongs to: the DSL's windowed count
 * would drop it and only count it in a metric, so the late router decides first, with the same rule
 * and a stream time at least as new as the count's (see {@link LateRouter}). With {@code
 * exactly_once_v2} the count, both side topics and the input offsets commit in one transaction.
 */
public final class SalesPerMinuteTopology {

  /** The bookings topic of Module 27, written by Module 28's relays. */
  public static final String SOURCE_TOPIC = "seatwave.bookings";

  /** The dashboard's topic, compacted: the latest count of each show and minute. */
  public static final String OUTPUT_TOPIC = "seatwave.analytics.sales-per-minute";

  /** Confirms that came after their window's grace, kept for the batch's correction. */
  public static final String LATE_TOPIC = "seatwave.analytics.late-bookings";

  /** Records the reader cannot use, kept untouched with the reason. */
  public static final String DEAD_LETTER_TOPIC = "seatwave.bookings.dlq";

  /** The job's id, also its consumer group and the prefix of its internal topics. */
  public static final String APPLICATION_ID = "analytics-sales";

  /** The window store, whose changelog topic makes the counts survive a restart. */
  public static final String STORE = "sales-per-minute-store";

  /** Tumbling windows of one minute of event time. */
  public static final Duration WINDOW = Duration.ofMinutes(1);

  /**
   * Kafka Streams closes a window when stream time passes its end plus the grace: SeatWave's 10 s
   * watermark delay plus 2 minutes of allowed lateness, which closes it when {@code MinuteWindows}
   * does.
   */
  public static final Duration GRACE = Duration.ofSeconds(130);

  private static final String KEY_SEPARATOR = "@";

  private SalesPerMinuteTopology() {}

  /** The window key the output and the late topic use: {@code <event_id>@<minute start>}. */
  public static String windowKey(long eventId, Instant windowStart) {
    return eventId + KEY_SEPARATOR + windowStart;
  }

  /** The topology; build one per {@code KafkaStreams} instance. */
  public static Topology build() {
    StreamsBuilder builder = new StreamsBuilder();
    Map<String, KStream<String, Parsed>> read =
        builder.stream(
                SOURCE_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
                    .withTimestampExtractor(new OccurredAtExtractor()))
            .mapValues(raw -> new Parsed(raw, BookingEventJson.read(raw)), Named.as("read"))
            .split(Named.as("read-"))
            .branch((bookingId, parsed) -> parsed.isUnreadable(), Branched.as("unreadable"))
            .defaultBranch(Branched.as("readable"));
    read.get("read-unreadable")
        .processValues(DeadLetterHeaders::new, Named.as("dead-letter-headers"))
        .to(DEAD_LETTER_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    Map<String, KStream<String, Routed>> routed =
        read.get("read-readable")
            .filter((bookingId, parsed) -> parsed.event().isConfirm(), Named.as("confirms"))
            // Re-keying by show sends each record through a repartition topic, so one task owns
            // a show; the record keeps its occurred_at timestamp on the way.
            .selectKey(
                (bookingId, parsed) -> Long.toString(parsed.event().eventId()), Named.as("by-show"))
            .mapValues(Parsed::raw)
            .repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withName("by-show"))
            .processValues(LateRouter::new, Named.as("late-router"))
            .split(Named.as("window-"))
            .branch((show, route) -> route.isLate(), Branched.as("late"))
            .defaultBranch(Branched.as("open"));
    routed
        .get("window-late")
        .map(
            (show, route) ->
                KeyValue.pair(
                    windowKey(
                        Long.parseLong(show), Instant.ofEpochMilli(route.windowStartMillis())),
                    route.raw()),
            Named.as("late-by-window"))
        .to(LATE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    routed
        .get("window-open")
        .mapValues(Routed::raw)
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW, GRACE))
        .count(Materialized.as(STORE))
        .toStream()
        .map(
            (windowedShow, confirms) ->
                KeyValue.pair(
                    windowKey(
                        Long.parseLong(windowedShow.key()), windowedShow.window().startTime()),
                    confirms),
            Named.as("by-window"))
        .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    return builder.build();
  }

  /** The job's settings; the state folder holds the window store's local copy. */
  public static Properties settings(String bootstrapServers, Path stateFolder) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    properties.put(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        Objects.requireNonNull(bootstrapServers, "bootstrapServers"));
    properties.put(
        StreamsConfig.STATE_DIR_CONFIG,
        Objects.requireNonNull(stateFolder, "stateFolder").toAbsolutePath().toString());
    // State, side topics, output and offsets commit in one Kafka transaction: a restart neither
    // loses nor repeats a record's effect on a window (Section 6, "together").
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    return properties;
  }

  /** A record's raw value and what the reader made of it; never written to a topic. */
  private record Parsed(String raw, BookingEventJson.ReadResult result) {

    boolean isUnreadable() {
      return result instanceof BookingEventJson.ReadResult.Unreadable;
    }

    /** The event; called only on the readable branch, so anything else is a programming error. */
    BookingEvent event() {
      if (result instanceof BookingEventJson.ReadResult.Readable readable) {
        return readable.event();
      }
      throw new IllegalStateException("event() called on an unreadable record");
    }
  }

  /** A confirm, the window it belongs to, and whether that window's grace has passed. */
  record Routed(String raw, long windowStartMillis, boolean isLate) {}

  /**
   * Decides, before the windowed count, whether a confirm is too late for its window, with the
   * count's own rule: a window ending at {@code end} takes a record while {@code end + grace} is
   * after stream time. The task's stream time includes every record the count has seen and is never
   * older than the count's own, so a record this router lets through is never dropped by the count,
   * and a record it routes to the late topic is never counted: each confirm lands in exactly one of
   * the two.
   */
  static final class LateRouter implements FixedKeyProcessor<String, String, Routed> {

    private FixedKeyProcessorContext<String, Routed> context;

    @Override
    public void init(FixedKeyProcessorContext<String, Routed> context) {
      this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, String> record) {
      long windowMillis = WINDOW.toMillis();
      long windowStartMillis = record.timestamp() - Math.floorMod(record.timestamp(), windowMillis);
      long closesAtMillis = windowStartMillis + windowMillis + GRACE.toMillis();
      boolean isLate = closesAtMillis <= context.currentStreamTimeMs();
      context.forward(record.withValue(new Routed(record.value(), windowStartMillis, isLate)));
    }
  }

  /**
   * Adds Module 27's dead-letter headers ({@code error}, {@code attempts}, {@code source}, {@code
   * consumer-group}) and keeps the key and value untouched, so the record can be fixed and
   * replayed.
   */
  static final class DeadLetterHeaders implements FixedKeyProcessor<String, Parsed, String> {

    private FixedKeyProcessorContext<String, String> context;

    @Override
    public void init(FixedKeyProcessorContext<String, String> context) {
      this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, Parsed> record) {
      String reason =
          record.value().result() instanceof BookingEventJson.ReadResult.Unreadable unreadable
              ? unreadable.reason()
              : "readable";
      String source =
          context
              .recordMetadata()
              .map(meta -> meta.topic() + "/" + meta.partition() + "/" + meta.offset())
              .orElse(SOURCE_TOPIC);
      Headers headers = record.headers();
      headers.add("error", reason.getBytes(StandardCharsets.UTF_8));
      headers.add("attempts", "1".getBytes(StandardCharsets.UTF_8));
      headers.add("source", source.getBytes(StandardCharsets.UTF_8));
      headers.add("consumer-group", APPLICATION_ID.getBytes(StandardCharsets.UTF_8));
      context.forward(record.withValue(record.value().raw()).withHeaders(headers));
    }
  }

  /** Event time is the commit time inside the value, not the time the producer sent it. */
  static final class OccurredAtExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
      String value = record.value() instanceof String text ? text : null;
      return BookingEventJson.occurredAtMillis(value)
          .orElse(
              // An unreadable record must not move stream time, and must not be dropped for a
              // negative timestamp before it reaches the dead-letter branch: it takes the
              // partition's time, or its own send time when the partition has none yet.
              partitionTime >= 0 ? partitionTime : Math.max(0, record.timestamp()));
    }
  }
}
