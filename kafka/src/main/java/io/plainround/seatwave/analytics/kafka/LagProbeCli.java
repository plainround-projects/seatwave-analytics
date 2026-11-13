package io.plainround.seatwave.analytics.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Samples the stream job's lag once a second: the records of {@code seatwave.bookings} appended but
 * not yet committed by the group {@code analytics-sales}, and that lag in seconds at the
 * publisher's stated rate, which is what the alert "stream lag over 2 minutes" watches.
 *
 * <p>Usage: {@code --bootstrap 127.0.0.1:23192 --rate 1500 --seconds 120}
 */
public final class LagProbeCli {

  private static final Duration SAMPLE = Duration.ofSeconds(1);
  private static final Duration CALL_TIMEOUT = Duration.ofSeconds(10);
  private static final int PARTITIONS = 24;
  private static final int MAX_SECONDS = 3_600;
  private static final int MAX_RATE = 100_000;

  private LagProbeCli() {}

  public static void main(String[] arguments)
      throws InterruptedException, ExecutionException, TimeoutException {
    Options options =
        Options.parse(
            arguments, Map.of("bootstrap", "127.0.0.1:23192", "rate", "1500", "seconds", "120"));
    String bootstrap = options.text("bootstrap");
    int rate = options.integer("rate", 1, MAX_RATE);
    int seconds = options.integer("seconds", 1, MAX_SECONDS);
    if (!options.problems().isEmpty()) {
      options.problems().forEach(System.err::println);
      System.exit(2);
    }
    List<TopicPartition> partitions =
        java.util.stream.IntStream.range(0, PARTITIONS)
            .mapToObj(index -> new TopicPartition(SalesPerMinuteTopology.SOURCE_TOPIC, index))
            .toList();
    long largestLag = 0;
    int largestAtSecond = 0;
    try (Admin admin =
        Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
      for (int second = 1; second <= seconds; second++) {
        Thread.sleep(SAMPLE);
        long lag = lag(admin, partitions);
        System.out.printf(
            Locale.ROOT,
            "t=%3d s  lag %,7d records  = %6.1f s at %,d records/s%n",
            second,
            lag,
            (double) lag / rate,
            rate);
        if (lag > largestLag) {
          largestLag = lag;
          largestAtSecond = second;
        }
      }
    }
    System.out.printf(
        Locale.ROOT,
        "largest lag %,d records at t=%d s = %.1f s at %,d records/s (alert at 120 s)%n",
        largestLag,
        largestAtSecond,
        (double) largestLag / rate,
        rate);
  }

  private static long lag(Admin admin, List<TopicPartition> partitions)
      throws InterruptedException, ExecutionException, TimeoutException {
    Map<TopicPartition, OffsetAndMetadata> committed =
        admin
            .listConsumerGroupOffsets(SalesPerMinuteTopology.APPLICATION_ID)
            .partitionsToOffsetAndMetadata()
            .get(CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
        admin
            .listOffsets(
                partitions.stream()
                    .collect(Collectors.toMap(Function.identity(), part -> OffsetSpec.latest())))
            .all()
            .get(CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    long lag = 0;
    for (TopicPartition partition : partitions) {
      OffsetAndMetadata position = committed.get(partition);
      lag += ends.get(partition).offset() - (position == null ? 0 : position.offset());
    }
    return lag;
  }
}
