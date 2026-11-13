package io.plainround.seatwave.analytics;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Tickets sold per minute during the on-sale, counted four ways: by processing time, by event time
 * with SeatWave's watermark and lateness, under other watermark settings, and without removing
 * duplicates; then a sliding and a session window, and what an idle input does to the watermark.
 * Every time printed is virtual: it comes from the seeded stream, not from a clock.
 */
final class WindowsCli {

  private static final DateTimeFormatter CLOCK =
      DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT).withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter MINUTE =
      DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT).withZone(ZoneOffset.UTC);
  private static final double[] PERCENTILES = {50, 90, 99, 99.9, 100};
  private static final int SLIDING_MINUTES = 5;
  private static final Duration SESSION_GAP = Duration.ofMinutes(5);
  private static final int BYTES_PER_MESSAGE_ID = 16; // a UUID's 128 bits, before any map overhead
  private static final double PERCENT = 100.0;

  private WindowsCli() {}

  public static void main(String[] arguments) {
    OnSaleGenerator.Settings sale = OnSaleGenerator.Settings.nationalOnSale();
    List<Delivery> deliveries = new OnSaleGenerator(sale).generate();
    List<Instant> minutes = new ArrayList<>();
    for (int minute = 0; minute < sale.saleSeconds() / 60; minute++) {
      minutes.add(sale.saleStart().plus(Duration.ofMinutes(minute)));
    }
    printStream(sale, deliveries);
    Map<Instant, Long> truth = truthByMinute(deliveries);
    MinuteWindows chosen = run(MinuteWindows.Settings.seatWave(), sale, deliveries);
    printPerMinute(sale, deliveries, minutes, truth, chosen);
    printSettings(sale, deliveries, minutes, truth);
    printDuplicates(sale, deliveries, truth, chosen);
    printOtherWindows(minutes, chosen);
    printIdleEnd(sale, deliveries);
  }

  private static void printStream(OnSaleGenerator.Settings sale, List<Delivery> deliveries) {
    long confirms =
        deliveries.stream()
            .filter(delivery -> !delivery.isRedelivery() && delivery.event().isConfirm())
            .count();
    long cancels =
        deliveries.stream()
            .filter(delivery -> !delivery.isRedelivery() && !delivery.event().isConfirm())
            .count();
    long redelivered = deliveries.stream().filter(Delivery::isRedelivery).count();
    System.out.println("Part 1. The stream analytics-sales reads (seeded, virtual time)");
    System.out.println(
        String.format(
            Locale.ROOT,
            "  %,d confirms in %d s from %s UTC, %d shows, %d partitions; %,d cancels; %,d records"
                + " read again after rebalances; %,d deliveries",
            confirms,
            sale.saleSeconds(),
            CLOCK.format(sale.saleStart()),
            sale.eventCount(),
            sale.partitions(),
            cancels,
            redelivered,
            deliveries.size()));
    long[] outOfOrderMillis = new long[deliveries.size()];
    long[] delayMillis = new long[deliveries.size()];
    Instant largestSeen = Instant.MIN;
    for (int index = 0; index < deliveries.size(); index++) {
      Delivery delivery = deliveries.get(index);
      Instant eventTime = delivery.event().occurredAt();
      largestSeen = eventTime.isAfter(largestSeen) ? eventTime : largestSeen;
      outOfOrderMillis[index] = Duration.between(eventTime, largestSeen).toMillis();
      delayMillis[index] = delivery.delay().toMillis();
    }
    System.out.println(
        "  how far behind the largest event time already seen each record arrives"
            + " (out-of-orderness):");
    System.out.println("    " + percentiles(outOfOrderMillis));
    System.out.println(
        String.format(
            Locale.ROOT,
            "    behind by more than 10 s: %s; more than 2 min: %s",
            share(outOfOrderMillis, Duration.ofSeconds(10).toMillis()),
            share(outOfOrderMillis, Duration.ofMinutes(2).toMillis())));
    System.out.println("  delay from commit to read (processing time minus event time):");
    System.out.println("    " + percentiles(delayMillis));
  }

  private static void printPerMinute(
      OnSaleGenerator.Settings sale,
      List<Delivery> deliveries,
      List<Instant> minutes,
      Map<Instant, Long> truth,
      MinuteWindows chosen) {
    MinuteWindows.Settings settings = MinuteWindows.Settings.seatWave();
    Map<Instant, Long> byProcessingTime = new HashMap<>();
    for (Delivery delivery : deliveries) {
      if (!delivery.isRedelivery() && delivery.event().isConfirm()) {
        byProcessingTime.merge(chosen.windowStartOf(delivery.processedAt()), 1L, Long::sum);
      }
    }
    Map<Instant, Instant> firedAt = new HashMap<>();
    Map<Instant, Instant> closedAt = new HashMap<>();
    MinuteWindows replay = new MinuteWindows(settings, sale.partitions());
    for (Delivery delivery : deliveries) {
      replay.accept(delivery);
      for (Instant minute : minutes) {
        Instant end = minute.plus(settings.windowSize());
        if (!end.isAfter(replay.watermark())) {
          firedAt.putIfAbsent(minute, delivery.processedAt());
        }
        if (!end.plus(settings.allowedLateness()).isAfter(replay.watermark())) {
          closedAt.putIfAbsent(minute, delivery.processedAt());
        }
      }
    }
    Map<Instant, Long> first = confirmsByMinute(chosen.firstResults());
    Map<Instant, Long> last = confirmsByMinute(chosen.results());
    Map<Instant, Long> tooLate = new HashMap<>();
    chosen.tooLate().stream()
        .filter(delivery -> delivery.event().isConfirm())
        .forEach(delivery ->
            tooLate.merge(chosen.windowStartOf(delivery.event().occurredAt()), 1L, Long::sum));
    System.out.println();
    System.out.println(
        "Part 2. Confirms per minute, all shows: watermark 10 s behind, allowed lateness 2 min");
    System.out.println(
        "  minute   truth    by processing time   event time: first result (at)   after lateness"
            + " (closed at)   too late");
    for (Instant minute : minutes) {
      System.out.println(
          String.format(
              Locale.ROOT,
              "  %s  %,8d  %,10d  %+8d   %,8d (%s)          %,8d (%s)       %,6d",
              MINUTE.format(minute),
              truth.getOrDefault(minute, 0L),
              byProcessingTime.getOrDefault(minute, 0L),
              byProcessingTime.getOrDefault(minute, 0L) - truth.getOrDefault(minute, 0L),
              first.getOrDefault(minute, 0L),
              CLOCK.format(firedAt.get(minute)),
              last.getOrDefault(minute, 0L),
              CLOCK.format(closedAt.get(minute)),
              tooLate.getOrDefault(minute, 0L)));
    }
  }

  private static void printSettings(
      OnSaleGenerator.Settings sale,
      List<Delivery> deliveries,
      List<Instant> minutes,
      Map<Instant, Long> truth) {
    long allRecords = deliveries.stream().filter(delivery -> !delivery.isRedelivery()).count();
    long truthTotal = minutes.stream().mapToLong(minute -> truth.getOrDefault(minute, 0L)).sum();
    Duration minute = Duration.ofMinutes(1);
    Duration[][] choices = {
      {Duration.ZERO, Duration.ZERO},
      {Duration.ofSeconds(10), Duration.ZERO},
      {Duration.ofMinutes(2), Duration.ZERO},
      {Duration.ofSeconds(10), Duration.ofMinutes(2)},
      {Duration.ofSeconds(10), Duration.ofMinutes(6)},
    };
    System.out.println();
    System.out.println("Part 3. Five watermark and lateness settings on the same stream");
    System.out.println(
        "  watermark  lateness  first result  first results    too late (share of records)  late"
            + " updates  ids held at most");
    for (Duration[] choice : choices) {
      MinuteWindows windows =
          run(new MinuteWindows.Settings(minute, choice[0], choice[1], true), sale, deliveries);
      long firstTotal =
          minutes.stream()
              .mapToLong(start -> confirmsByMinute(windows.firstResults()).getOrDefault(start, 0L))
              .sum();
      MinuteWindows.Stats stats = windows.stats();
      System.out.println(
          String.format(
              Locale.ROOT,
              "  %8s  %8s  end + %-6s  %6.2f%% of truth   %,7d (%.3f%%)            %,9d     %,9d",
              words(choice[0]),
              words(choice[1]),
              words(choice[0]),
              PERCENT * firstTotal / truthTotal,
              stats.tooLate(),
              PERCENT * stats.tooLate() / allRecords,
              stats.lateAccepted(),
              stats.maxIdsHeld()));
    }
  }

  private static void printDuplicates(
      OnSaleGenerator.Settings sale,
      List<Delivery> deliveries,
      Map<Instant, Long> truth,
      MinuteWindows chosen) {
    MinuteWindows.Settings base = MinuteWindows.Settings.seatWave();
    MinuteWindows counting =
        run(
            new MinuteWindows.Settings(
                base.windowSize(), base.watermarkDelay(), base.allowedLateness(), false),
            sale,
            deliveries);
    long truthTotal = truth.values().stream().mapToLong(Long::longValue).sum();
    long withoutTooLate =
        truthTotal
            - chosen.tooLate().stream().filter(delivery -> delivery.event().isConfirm()).count();
    long deduplicated =
        confirmsByMinute(chosen.results()).values().stream().mapToLong(Long::longValue).sum();
    long counted =
        confirmsByMinute(counting.results()).values().stream().mapToLong(Long::longValue).sum();
    MinuteWindows.Stats stats = chosen.stats();
    System.out.println();
    System.out.println(
        "Part 4. Duplicates from at-least-once delivery (two rebalances, 4 partitions each, up to"
            + " 100 records each)");
    System.out.println(
        String.format(
            Locale.ROOT,
            "  confirms the stream should count (truth minus too late): %,d",
            withoutTooLate));
    long confirmCopies =
        deliveries.stream()
            .filter(delivery -> delivery.isRedelivery() && delivery.event().isConfirm())
            .count();
    System.out.println(
        String.format(
            Locale.ROOT,
            "  without dedupe: %,d (%+,d)   with dedupe by message_id: %,d (%+,d)",
            counted,
            counted - withoutTooLate,
            deduplicated,
            deduplicated - withoutTooLate));
    System.out.println(
        String.format(
            Locale.ROOT,
            "  repeats dropped: %,d (%,d confirms, %,d cancels)",
            stats.duplicatesDropped(),
            confirmCopies,
            stats.duplicatesDropped() - confirmCopies));
    System.out.println(
        String.format(
            Locale.ROOT,
            "  dedupe state: at most %,d message ids held at once, %,d bytes as raw 16-byte ids;"
                + " %,d still held in the %d windows open at the end",
            stats.maxIdsHeld(),
            stats.maxIdsHeld() * BYTES_PER_MESSAGE_ID,
            stats.idsHeldNow(),
            stats.windowsOpen()));
  }

  private static void printOtherWindows(List<Instant> minutes, MinuteWindows chosen) {
    Map<Instant, Long> perMinute = confirmsByMinute(chosen.results());
    System.out.println();
    System.out.println(
        "Part 5. A sliding window (5 minutes, every minute) and a session window (gap 5 minutes)");
    System.out.println("  window ending   confirms in the last 5 minutes");
    for (int index = SLIDING_MINUTES - 1; index < minutes.size(); index += 2) {
      long sum = 0;
      for (int back = 0; back < SLIDING_MINUTES; back++) {
        sum += perMinute.getOrDefault(minutes.get(index - back), 0L);
      }
      System.out.println(
          String.format(
              Locale.ROOT,
              "  %s           %,d",
              MINUTE.format(minutes.get(index).plus(Duration.ofMinutes(1))),
              sum));
    }
    System.out.println(
        "  (each record belongs to 5 sliding windows, so the job keeps 5 counters where a tumbling"
            + " window keeps 1)");
    Instant start = minutes.getFirst();
    List<Instant> oneFan =
        List.of(
            start.plusSeconds(5),
            start.plusSeconds(40),
            start.plusSeconds(130),
            start.plusSeconds(660),
            start.plusSeconds(690));
    System.out.println("  one fan's confirms at " + oneFan.stream().map(CLOCK::format).toList());
    System.out.println("  sessions with a 5-minute gap: " + sessions(oneFan));
  }

  private static void printIdleEnd(OnSaleGenerator.Settings sale, List<Delivery> deliveries) {
    MinuteWindows windows = new MinuteWindows(MinuteWindows.Settings.seatWave(), sale.partitions());
    deliveries.forEach(windows::accept);
    Instant lastEventTime =
        deliveries.stream()
            .map(delivery -> delivery.event().occurredAt())
            .max(Instant::compareTo)
            .orElseThrow();
    System.out.println();
    System.out.println(
        "Part 6. After the last record: the watermark moves only when records arrive");
    System.out.println(
        String.format(
            Locale.ROOT,
            "  last event time %s, watermark %s, windows still open %,d",
            CLOCK.format(lastEventTime),
            CLOCK.format(windows.watermark()),
            windows.stats().windowsOpen()));
    windows.endOfInput();
    System.out.println(
        String.format(
            Locale.ROOT,
            "  end of input declared (bounded input): windows still open %d",
            windows.stats().windowsOpen()));
  }

  private static MinuteWindows run(
      MinuteWindows.Settings settings, OnSaleGenerator.Settings sale, List<Delivery> deliveries) {
    MinuteWindows windows = new MinuteWindows(settings, sale.partitions());
    deliveries.forEach(windows::accept);
    return windows;
  }

  private static Map<Instant, Long> truthByMinute(List<Delivery> deliveries) {
    long minuteMillis = Duration.ofMinutes(1).toMillis();
    Map<Instant, Long> truth = new HashMap<>();
    for (Delivery delivery : deliveries) {
      if (!delivery.isRedelivery() && delivery.event().isConfirm()) {
        long millis = delivery.event().occurredAt().toEpochMilli();
        truth.merge(
            Instant.ofEpochMilli(Math.floorDiv(millis, minuteMillis) * minuteMillis),
            1L,
            Long::sum);
      }
    }
    return truth;
  }

  private static Map<Instant, Long> confirmsByMinute(
      Map<MinuteWindows.WindowKey, MinuteWindows.Totals> windows) {
    Map<Instant, Long> perMinute = new HashMap<>();
    windows.forEach(
        (key, totals) -> perMinute.merge(key.windowStart(), totals.confirms(), Long::sum));
    return perMinute;
  }

  private static List<List<String>> sessions(List<Instant> times) {
    List<List<String>> sessions = new ArrayList<>();
    Instant previous = Instant.MIN;
    for (Instant time : times) {
      if (sessions.isEmpty() || Duration.between(previous, time).compareTo(SESSION_GAP) > 0) {
        sessions.add(new ArrayList<>());
      }
      sessions.getLast().add(CLOCK.format(time));
      previous = time;
    }
    return sessions;
  }

  private static String percentiles(long[] millis) {
    long[] sorted = millis.clone();
    Arrays.sort(sorted);
    StringBuilder line = new StringBuilder();
    for (double percentile : PERCENTILES) {
      int index =
          (int) Math.min(sorted.length - 1, Math.ceil(percentile / PERCENT * sorted.length) - 1);
      String label =
          percentile == PERCENT
              ? "max"
              : "p"
                  + new BigDecimal(String.valueOf(percentile)).stripTrailingZeros().toPlainString();
      line.append(String.format(Locale.ROOT, "%s %,d ms   ", label, sorted[Math.max(0, index)]));
    }
    return line.toString().strip();
  }

  private static String share(long[] millis, long limitMillis) {
    long over = Arrays.stream(millis).filter(value -> value > limitMillis).count();
    return String.format(Locale.ROOT, "%,d (%.3f%%)", over, PERCENT * over / millis.length);
  }

  private static String words(Duration duration) {
    return duration.toSeconds() % 60 == 0 && !duration.isZero()
        ? duration.toMinutes() + " min"
        : duration.toSeconds() + " s";
  }
}
