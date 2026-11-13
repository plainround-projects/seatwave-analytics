package io.plainround.seatwave.analytics;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * A seeded, deterministic stream of booking events as {@code analytics-sales} would read them
 * during one on-sale: confirms at a fixed rate, some canceled later, most published within 400 ms,
 * 1 percent up to 2 minutes late, one shard's publisher stalled for 5 minutes, the job paused for a
 * deploy, and two rebalances that deliver the last uncommitted batch of some partitions again.
 *
 * <p>Thread-safety: not shared; {@link #generate()} builds a new list each call.
 */
public final class OnSaleGenerator {

  private static final int SECTIONS = 20; // Module 18: 20 sections of 1,000 seats per arena event
  private static final int SEATS_PER_SECTION = 1_000;
  private static final int MILLIS_PER_SECOND = 1_000;
  private static final int PERCENT = 100;
  private static final int UUID_VERSION_SHIFT = 12;
  private static final long VERSION_7 = 0x7L;
  private static final long VERSION_4 = 0x4L;
  private static final long LOW_TWELVE_BITS = 0x0fffL;
  private static final long VARIANT_CLEAR_MASK = 0x3fffffffffffffffL;
  private static final long VARIANT_RFC = 0x8000000000000000L;
  private static final int MILLIS_SHIFT = 16;
  private static final long VERSION_NIBBLE_MASK = 0xffffffffffff0fffL;

  /**
   * Everything that shapes the stream. Times are UTC instants; durations are positive.
   *
   * @param seed the random seed, so every run prints the same numbers
   * @param saleStart when the on-sale opens (event time)
   * @param saleSeconds how long confirms arrive
   * @param confirmsPerSecond the confirm rate
   * @param eventCount shows on sale (ids 1 to eventCount)
   * @param customerCount fans who may book (ids 1 to customerCount)
   * @param partitions partitions of {@code seatwave.bookings}
   * @param shards booking shards (Module 18)
   * @param latePercent share of events published 1 s to 2 minutes late
   * @param cancelPercent share of bookings canceled 1 to 45 minutes after the confirm
   * @param stalledShard the shard whose publisher stalls
   * @param stallStart first commit time held back on that shard
   * @param stallEnd when that shard's publisher resumes
   * @param jobPauseStart processing time at which the job stops for a deploy
   * @param jobPauseEnd processing time at which it starts again
   * @param catchUpPerSecond records a second the job reads while it has a backlog
   * @param redeliveryBatch records per partition read again after a rebalance (max.poll.records)
   */
  public record Settings(
      long seed,
      Instant saleStart,
      int saleSeconds,
      int confirmsPerSecond,
      int eventCount,
      int customerCount,
      int partitions,
      int shards,
      int latePercent,
      int cancelPercent,
      int stalledShard,
      Instant stallStart,
      Instant stallEnd,
      Instant jobPauseStart,
      Instant jobPauseEnd,
      int catchUpPerSecond,
      int redeliveryBatch) {

    public Settings {
      Objects.requireNonNull(saleStart, "saleStart");
      Objects.requireNonNull(stallStart, "stallStart");
      Objects.requireNonNull(stallEnd, "stallEnd");
      Objects.requireNonNull(jobPauseStart, "jobPauseStart");
      Objects.requireNonNull(jobPauseEnd, "jobPauseEnd");
      if (saleSeconds < 1 || confirmsPerSecond < 1 || eventCount < 1 || customerCount < 1) {
        throw new IllegalArgumentException(
            "sale length, rate, events and customers must be positive");
      }
      if (partitions < 1 || shards < 1 || stalledShard < 0 || stalledShard >= shards) {
        throw new IllegalArgumentException("partitions, shards or stalledShard out of range");
      }
      if (latePercent < 0
          || latePercent > PERCENT
          || cancelPercent < 0
          || cancelPercent > PERCENT) {
        throw new IllegalArgumentException("percentages must be 0 to 100");
      }
      if (!stallEnd.isAfter(stallStart) || !jobPauseEnd.isAfter(jobPauseStart)) {
        throw new IllegalArgumentException("each stall and pause must end after it starts");
      }
      if (catchUpPerSecond <= confirmsPerSecond || redeliveryBatch < 0) {
        throw new IllegalArgumentException(
            "catch-up must beat arrivals, or a backlog never drains");
      }
    }

    /**
     * Module 27's national on-sale: 1,500 confirms a second for 900 s, 24 partitions, 64 shards.
     */
    public static Settings nationalOnSale() {
      return scaled(1_500);
    }

    /** The same on-sale shape at another confirm rate (tests use a smaller one). */
    public static Settings scaled(int confirmsPerSecond) {
      Instant start = Instant.parse("2026-11-20T19:00:00Z");
      return new Settings(
          31L,
          start,
          900, // Module 2: the 15-minute sale window
          confirmsPerSecond,
          70, // 70 arena shows of 20,000 seats: 1,400,000 seats for 1,350,000 confirms
          1_000_000, // Module 3: the tour's 1,000,000 fans
          24, // Module 27: seatwave.bookings has 24 partitions
          64, // Module 18: 64 booking shards
          1, // the walkthrough's constraint: 1 percent up to 2 minutes late
          2, // assumed: 2 percent of bookings canceled within 45 minutes
          37, // Module 27's shard-37
          start.plusSeconds(420),
          start.plusSeconds(720),
          start.plusSeconds(300),
          start.plusSeconds(360),
          confirmsPerSecond * 10 / 3, // assumed: the job reads 3.3 times the peak rate when behind
          100); // Module 27: max.poll.records=100
    }
  }

  private static final Duration NORMAL_DELAY_MIN = Duration.ofMillis(20);
  private static final int NORMAL_DELAY_SPREAD_MILLIS = 380; // published 20 to 400 ms after commit
  private static final int LATE_DELAY_MIN_MILLIS = 1_000;
  private static final int LATE_DELAY_SPREAD_MILLIS = 119_000; // late ones: 1 s to 120 s
  private static final int CANCEL_AFTER_MIN_SECONDS = 60;
  private static final int CANCEL_AFTER_SPREAD_SECONDS = 2_640; // canceled 1 to 45 minutes later
  private static final Duration RELAY_CATCH_UP = Duration.ofSeconds(4);
  private static final Duration REBALANCE_DELAY = Duration.ofSeconds(3);
  private static final Duration FIRST_REBALANCE = Duration.ofSeconds(270);
  private static final Duration SECOND_REBALANCE = Duration.ofSeconds(615);
  private static final Set<Integer> FIRST_REBALANCE_PARTITIONS = Set.of(2, 3, 14, 15);
  private static final Set<Integer> SECOND_REBALANCE_PARTITIONS = Set.of(8, 9, 20, 21);

  private record Pending(BookingEvent event, Instant appendedAt) {}

  private final Settings settings;
  private final String[] seatLabels;

  public OnSaleGenerator(Settings settings) {
    this.settings = Objects.requireNonNull(settings, "settings");
    this.seatLabels = new String[SECTIONS * SEATS_PER_SECTION];
    for (int labelIndex = 0; labelIndex < seatLabels.length; labelIndex++) {
      char section = (char) ('A' + labelIndex / SEATS_PER_SECTION);
      seatLabels[labelIndex] = section + "-" + (labelIndex % SEATS_PER_SECTION + 1);
    }
  }

  /** Every delivery the job reads, in processing order, redeliveries included. */
  public List<Delivery> generate() {
    Random random = new Random(settings.seed());
    List<Pending> pending = new ArrayList<>();
    for (int second = 0; second < settings.saleSeconds(); second++) {
      for (int confirmIndex = 0; confirmIndex < settings.confirmsPerSecond(); confirmIndex++) {
        addBooking(random, second, pending);
      }
    }
    // The broker appends in arrival order; ties broken by booking id so every run is identical.
    pending.sort(
        Comparator.comparing(Pending::appendedAt).thenComparing(item -> item.event().bookingId()));
    List<Delivery> deliveries = readInOrder(pending);
    deliveries.addAll(redeliver(deliveries, FIRST_REBALANCE, FIRST_REBALANCE_PARTITIONS));
    deliveries.addAll(redeliver(deliveries, SECOND_REBALANCE, SECOND_REBALANCE_PARTITIONS));
    // A stable sort keeps originals ahead of copies read at the same instant.
    deliveries.sort(Comparator.comparing(Delivery::processedAt));
    return deliveries;
  }

  private void addBooking(Random random, int second, List<Pending> pending) {
    Instant occurredAt =
        settings.saleStart().plusSeconds(second).plusMillis(random.nextInt(MILLIS_PER_SECOND));
    long eventId = 1 + random.nextInt(settings.eventCount());
    long customerId = 1 + random.nextInt(settings.customerCount());
    int shard = random.nextInt(settings.shards());
    UUID bookingId = versionSevenUuid(occurredAt, random);
    String seatLabel = seatLabels[random.nextInt(seatLabels.length)];
    BookingEvent confirm =
        new BookingEvent(
            randomUuid(random),
            BookingEvent.Type.BOOKING_CONFIRMED,
            bookingId,
            1,
            customerId,
            eventId,
            seatLabel,
            occurredAt);
    pending.add(new Pending(confirm, appendTime(random, shard, occurredAt)));
    if (random.nextInt(PERCENT) < settings.cancelPercent()) {
      Instant canceledAt =
          occurredAt.plusSeconds(
              CANCEL_AFTER_MIN_SECONDS + random.nextInt(CANCEL_AFTER_SPREAD_SECONDS));
      BookingEvent cancel =
          new BookingEvent(
              randomUuid(random),
              BookingEvent.Type.BOOKING_CANCELED,
              bookingId,
              2,
              customerId,
              eventId,
              seatLabel,
              canceledAt);
      pending.add(new Pending(cancel, appendTime(random, shard, canceledAt)));
    }
  }

  /** When the broker stores an event committed at {@code occurredAt} on {@code shard}. */
  private Instant appendTime(Random random, int shard, Instant occurredAt) {
    boolean isLate = random.nextInt(PERCENT) < settings.latePercent();
    Instant normal =
        isLate
            ? occurredAt.plusMillis(
                LATE_DELAY_MIN_MILLIS + random.nextInt(LATE_DELAY_SPREAD_MILLIS))
            : occurredAt
                .plus(NORMAL_DELAY_MIN)
                .plusMillis(random.nextInt(NORMAL_DELAY_SPREAD_MILLIS));
    boolean isHeldBack =
        shard == settings.stalledShard()
            && !occurredAt.isBefore(settings.stallStart())
            && occurredAt.isBefore(settings.stallEnd());
    if (!isHeldBack) {
      return normal;
    }
    // The stalled publisher sends its backlog in commit order, 5 minutes squeezed into 4 seconds.
    long stallNanos = Duration.between(settings.stallStart(), settings.stallEnd()).toNanos();
    long intoStallNanos = Duration.between(settings.stallStart(), occurredAt).toNanos();
    long squeeze = stallNanos / RELAY_CATCH_UP.toNanos();
    Instant released = settings.stallEnd().plusNanos(intoStallNanos / squeeze);
    return released.isAfter(normal) ? released : normal;
  }

  /** Offsets in append order, and the job's read time with its deploy pause and catch-up. */
  private List<Delivery> readInOrder(List<Pending> pending) {
    long[] nextOffset = new long[settings.partitions()];
    long nanosPerRecordWhenBehind = Duration.ofSeconds(1).toNanos() / settings.catchUpPerSecond();
    Instant readCursor = Instant.MIN;
    List<Delivery> deliveries = new ArrayList<>(pending.size());
    for (Pending item : pending) {
      int partition = Math.floorMod(item.event().bookingId().hashCode(), settings.partitions());
      Instant readAt = item.appendedAt();
      if (!readAt.isBefore(settings.jobPauseStart()) && readAt.isBefore(settings.jobPauseEnd())) {
        readAt = settings.jobPauseEnd();
      }
      boolean isBehind = readCursor.isAfter(readAt);
      if (isBehind) {
        readAt = readCursor;
      }
      // While a backlog exists, each record costs the job 1 / catchUpPerSecond of a second.
      readCursor =
          readAt.isAfter(item.appendedAt()) ? readAt.plusNanos(nanosPerRecordWhenBehind) : readAt;
      deliveries.add(
          new Delivery(
              item.event(), partition, nextOffset[partition]++, item.appendedAt(), readAt, false));
    }
    return deliveries;
  }

  /** The last uncommitted batch of each partition, read again after a member left at {@code at}. */
  private List<Delivery> redeliver(
      List<Delivery> deliveries, Duration at, Set<Integer> partitions) {
    Instant rebalanceAt = settings.saleStart().plus(at);
    Instant readAgainAt = rebalanceAt.plus(REBALANCE_DELAY);
    List<Delivery> copies = new ArrayList<>();
    for (int partition : partitions.stream().sorted().toList()) {
      List<Delivery> readBefore =
          deliveries.stream()
              .filter(delivery -> delivery.partition() == partition && !delivery.isRedelivery())
              .filter(delivery -> !delivery.processedAt().isAfter(rebalanceAt))
              .toList();
      int from = Math.max(0, readBefore.size() - settings.redeliveryBatch());
      for (Delivery original : readBefore.subList(from, readBefore.size())) {
        copies.add(
            new Delivery(
                original.event(),
                partition,
                original.offset(),
                original.appendedAt(),
                readAgainAt,
                true));
      }
    }
    return copies;
  }

  private static UUID versionSevenUuid(Instant occurredAt, Random random) {
    long high =
        (occurredAt.toEpochMilli() << MILLIS_SHIFT)
            | (VERSION_7 << UUID_VERSION_SHIFT)
            | (random.nextLong() & LOW_TWELVE_BITS);
    long low = (random.nextLong() & VARIANT_CLEAR_MASK) | VARIANT_RFC;
    return new UUID(high, low);
  }

  private static UUID randomUuid(Random random) {
    long high = (random.nextLong() & VERSION_NIBBLE_MASK) | (VERSION_4 << UUID_VERSION_SHIFT);
    long low = (random.nextLong() & VARIANT_CLEAR_MASK) | VARIANT_RFC;
    return new UUID(high, low);
  }
}
