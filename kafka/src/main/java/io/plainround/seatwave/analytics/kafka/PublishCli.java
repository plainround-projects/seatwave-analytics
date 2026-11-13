package io.plainround.seatwave.analytics.kafka;

import io.plainround.seatwave.analytics.Delivery;
import io.plainround.seatwave.analytics.OnSaleGenerator;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Publishes the seeded on-sale to {@code seatwave.bookings}, standing in for Module 28's relays:
 * every original record (not the generator's consumer-side redeliveries) in the broker's append
 * order, so the stalled publisher's backlog of shard-37 arrives late exactly as the generator
 * planned, at a fixed wall-clock rate. Event time travels inside each value; the record's own
 * timestamp is the send time. {@code --unparseable n} spreads n values that are not JSON among
 * them, for the dead-letter path.
 *
 * <p>Usage: {@code --bootstrap 127.0.0.1:23192 --confirms-per-second 150 --rate 1500 --unparseable
 * 3}
 */
public final class PublishCli {

  private static final Duration SEND_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration TICK = Duration.ofMillis(100);
  private static final int TICKS_PER_SECOND = 10;
  private static final int MAX_RATE = 100_000;
  private static final int MAX_CONFIRMS_PER_SECOND = 1_500; // the module's peak; 3 GB of heap

  private PublishCli() {}

  public static void main(String[] arguments) throws InterruptedException, TimeoutException {
    Options options =
        Options.parse(
            arguments,
            Map.of(
                "bootstrap", "127.0.0.1:23192",
                "confirms-per-second", "150",
                "rate", "1500",
                "unparseable", "0"));
    String bootstrap = options.text("bootstrap");
    int confirmsPerSecond = options.integer("confirms-per-second", 1, MAX_CONFIRMS_PER_SECOND);
    int rate = options.integer("rate", TICKS_PER_SECOND, MAX_RATE);
    int unparseable = options.integer("unparseable", 0, MAX_RATE);
    if (!options.problems().isEmpty()) {
      options.problems().forEach(System.err::println);
      System.exit(2);
    }
    List<Delivery> appendOrder =
        new OnSaleGenerator(OnSaleGenerator.Settings.scaled(confirmsPerSecond))
            .generate().stream()
                .filter(delivery -> !delivery.isRedelivery())
                .sorted(
                    Comparator.comparing(Delivery::appendedAt)
                        .thenComparing(delivery -> delivery.event().bookingId()))
                .toList();
    StackClients.createTopics(
        bootstrap, Map.of(SalesPerMinuteTopology.SOURCE_TOPIC, 24), Map.of(), SEND_TIMEOUT);
    long startedNanos = System.nanoTime();
    AtomicLong failures = new AtomicLong();
    long confirms = appendOrder.stream().filter(delivery -> delivery.event().isConfirm()).count();
    try (KafkaProducer<String, String> producer =
        StackClients.producer(bootstrap, "on-sale-publisher", SEND_TIMEOUT)) {
      int perTick = rate / TICKS_PER_SECOND;
      int spacing = unparseable == 0 ? Integer.MAX_VALUE : appendOrder.size() / (unparseable + 1);
      int sentUnparseable = 0;
      for (int index = 0; index < appendOrder.size(); index++) {
        if (index > 0 && index % perTick == 0) {
          sleepUntil(startedNanos + (index / perTick) * TICK.toNanos());
        }
        Delivery delivery = appendOrder.get(index);
        String bookingId = delivery.event().bookingId().toString();
        producer.send(
            new ProducerRecord<>(
                SalesPerMinuteTopology.SOURCE_TOPIC,
                bookingId,
                BookingEventJson.write(delivery.event())),
            (metadata, error) -> {
              if (error != null) {
                failures.incrementAndGet();
              }
            });
        if (index > 0 && index % spacing == 0 && sentUnparseable < unparseable) {
          sentUnparseable++;
          producer.send(
              new ProducerRecord<>(
                  SalesPerMinuteTopology.SOURCE_TOPIC,
                  "unparseable-" + sentUnparseable,
                  "{\"type\":\"BookingConfirmed\",\"booking_id\":"));
        }
      }
      producer.flush();
      double seconds = (System.nanoTime() - startedNanos) / 1e9;
      System.out.printf(
          Locale.ROOT,
          "published %,d records (%,d confirms, %,d cancels) and %d unparseable to %s in %.1f s:"
              + " %,.0f records/s (planned %,d); send failures %d%n",
          appendOrder.size(),
          confirms,
          appendOrder.size() - confirms,
          sentUnparseable,
          SalesPerMinuteTopology.SOURCE_TOPIC,
          seconds,
          appendOrder.size() / seconds,
          rate,
          failures.get());
    }
    if (failures.get() > 0) {
      System.exit(1);
    }
  }

  private static void sleepUntil(long deadlineNanos) throws InterruptedException {
    long waitNanos = deadlineNanos - System.nanoTime();
    if (waitNanos > 0) {
      Thread.sleep(Duration.ofNanos(waitNanos));
    }
  }
}
