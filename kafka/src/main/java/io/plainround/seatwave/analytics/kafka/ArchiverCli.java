package io.plainround.seatwave.analytics.kafka;

import io.plainround.seatwave.analytics.BookingEvent;
import io.plainround.seatwave.analytics.warehouse.Lake;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * The consumer group {@code bookings-archiver}: reads {@code seatwave.bookings} and appends every
 * readable booking event to the lake as Parquet, committing its offsets only after the files are
 * written (at least once: a crash in between writes those records again, and the nightly load keeps
 * one row per booking). Records the reader cannot use are left to the stream job's dead-letter
 * topic and only counted here. Stops after a quiet period with no records.
 *
 * <p>Usage: {@code --bootstrap 127.0.0.1:23192 --lake lake --quiet-seconds 20}
 */
public final class ArchiverCli {

  private static final String GROUP = "bookings-archiver";
  private static final Duration POLL = Duration.ofMillis(500);
  private static final int MAX_POLL_RECORDS = 5_000; // one Parquet file per partition per poll
  private static final int MAX_QUIET_SECONDS = 3_600;

  private ArchiverCli() {}

  public static void main(String[] arguments) throws SQLException, IOException {
    Options options =
        Options.parse(
            arguments,
            Map.of("bootstrap", "127.0.0.1:23192", "lake", "lake", "quiet-seconds", "20"));
    String bootstrap = options.text("bootstrap");
    Path lakeRoot = options.path("lake");
    Duration quiet = Duration.ofSeconds(options.integer("quiet-seconds", 1, MAX_QUIET_SECONDS));
    if (!options.problems().isEmpty()) {
      options.problems().forEach(System.err::println);
      System.exit(2);
    }
    long archived = 0;
    long unreadable = 0;
    long startedNanos = System.nanoTime();
    try (Lake lake = Lake.open(lakeRoot);
        KafkaConsumer<String, String> consumer = consumer(bootstrap)) {
      consumer.subscribe(List.of(SalesPerMinuteTopology.SOURCE_TOPIC));
      long lastRecordNanos = System.nanoTime();
      while (System.nanoTime() - lastRecordNanos < quiet.toNanos()) {
        ConsumerRecords<String, String> records = consumer.poll(POLL);
        if (records.isEmpty()) {
          continue;
        }
        lastRecordNanos = System.nanoTime();
        List<BookingEvent> events = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> record : records) {
          switch (BookingEventJson.read(record.value())) {
            case BookingEventJson.ReadResult.Readable readable -> events.add(readable.event());
            case BookingEventJson.ReadResult.Unreadable skipped -> unreadable++;
          }
        }
        archived += lake.append(events);
        // After the files exist, never before: a crash here repeats records, never loses them.
        consumer.commitSync();
      }
    }
    double seconds = (System.nanoTime() - startedNanos) / 1e9 - quiet.toSeconds();
    System.out.printf(
        Locale.ROOT,
        "%s: archived %,d records into %s (%d Parquet files, %,d bytes); skipped %d unreadable;"
            + " %.1f s before the %d s quiet period%n",
        GROUP,
        archived,
        lakeRoot.resolve("bookings"),
        files(lakeRoot).count(),
        files(lakeRoot).mapToLong(ArchiverCli::size).sum(),
        unreadable,
        seconds,
        quiet.toSeconds());
  }

  private static KafkaConsumer<String, String> consumer(String bootstrap) {
    return new KafkaConsumer<>(
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ConsumerConfig.GROUP_ID_CONFIG, GROUP,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS),
        new StringDeserializer(),
        new StringDeserializer());
  }

  private static Stream<Path> files(Path lakeRoot) {
    try (Stream<Path> walk = Files.walk(lakeRoot.resolve("bookings"))) {
      return walk.filter(path -> path.toString().endsWith(".parquet")).toList().stream();
    } catch (IOException failed) {
      throw new UncheckedIOException("cannot list the lake under " + lakeRoot, failed);
    }
  }

  private static long size(Path file) {
    try {
      return Files.size(file);
    } catch (IOException failed) {
      throw new UncheckedIOException("cannot read the size of " + file, failed);
    }
  }
}
