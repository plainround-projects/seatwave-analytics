package io.plainround.seatwave.analytics;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The stream job crashes at 19:08:20 and restarts. Checkpoints are taken every 15 s of processing
 * time; the program restarts it five ways, from state and read positions saved together or apart,
 * and compares the dashboard's per-minute confirms with a run that never crashed. The dashboard
 * table is keyed by window, so a restarted job's result for a window replaces the old one.
 */
final class CheckpointCli {

  private static final DateTimeFormatter CLOCK =
      DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT).withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter MINUTE =
      DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT).withZone(ZoneOffset.UTC);
  private static final Duration CRASH_AFTER = Duration.ofSeconds(500); // 19:08:20
  private static final Duration EARLIER_CHECKPOINT = Duration.ofSeconds(480); // 19:08:00
  private static final Duration LATER_CHECKPOINT = Duration.ofSeconds(495); // 19:08:15
  private static final int FIRST_MINUTE_SHOWN = 6;
  private static final int LAST_MINUTE_SHOWN = 9;

  /** Which saved pieces the restarted job uses. */
  private enum Restart {
    TOGETHER("state and offsets of 19:08:15, saved together"),
    OFFSETS_NEWER("state of 19:08:00, offsets of 19:08:15"),
    STATE_NEWER("state of 19:08:15, offsets of 19:08:00"),
    STATE_NEWER_NO_DEDUPE("the same, job without dedupe"),
    STATE_IN_MEMORY_ONLY("empty state, offsets of 19:08:15");

    private final String label;

    Restart(String label) {
      this.label = label;
    }
  }

  private CheckpointCli() {}

  public static void main(String[] arguments) {
    OnSaleGenerator.Settings sale = OnSaleGenerator.Settings.nationalOnSale();
    List<Delivery> deliveries = new OnSaleGenerator(sale).generate();
    MinuteWindows.Settings withDedupe = MinuteWindows.Settings.seatWave();
    MinuteWindows.Settings withoutDedupe =
        new MinuteWindows.Settings(
            withDedupe.windowSize(),
            withDedupe.watermarkDelay(),
            withDedupe.allowedLateness(),
            false);
    Instant crashAt = sale.saleStart().plus(CRASH_AFTER);
    System.out.println(
        String.format(
            Locale.ROOT,
            "The job crashes at %s (processing time); checkpoints at %s and %s; the dashboard keeps"
                + " the last result per window",
            CLOCK.format(crashAt),
            CLOCK.format(sale.saleStart().plus(EARLIER_CHECKPOINT)),
            CLOCK.format(sale.saleStart().plus(LATER_CHECKPOINT))));
    StringBuilder header = new StringBuilder("  restart from                                   ");
    for (int minute = FIRST_MINUTE_SHOWN; minute <= LAST_MINUTE_SHOWN; minute++) {
      header.append(
          String.format(
              Locale.ROOT,
              "  %s ",
              MINUTE.format(sale.saleStart().plus(Duration.ofMinutes(minute)))));
    }
    System.out.println(header.append("   all minutes vs no crash"));
    for (Restart restart : Restart.values()) {
      MinuteWindows.Settings settings =
          restart == Restart.STATE_NEWER_NO_DEDUPE ? withoutDedupe : withDedupe;
      Map<Instant, Long> noCrash = perMinute(runWithoutCrash(settings, sale, deliveries).results());
      Map<Instant, Long> dashboard = runWithCrash(restart, settings, sale, deliveries, crashAt);
      StringBuilder line = new StringBuilder(String.format(Locale.ROOT, "  %-45s", restart.label));
      for (int minute = FIRST_MINUTE_SHOWN; minute <= LAST_MINUTE_SHOWN; minute++) {
        Instant start = sale.saleStart().plus(Duration.ofMinutes(minute));
        line.append(
            String.format(
                Locale.ROOT,
                " %+7d",
                dashboard.getOrDefault(start, 0L) - noCrash.getOrDefault(start, 0L)));
      }
      long difference =
          dashboard.values().stream().mapToLong(Long::longValue).sum()
              - noCrash.values().stream().mapToLong(Long::longValue).sum();
      System.out.println(line.append(String.format(Locale.ROOT, "   %+,9d", difference)));
    }
    System.out.println(
        "  (each cell: the dashboard's confirms for that minute minus the run without a crash)");
  }

  private static MinuteWindows runWithoutCrash(
      MinuteWindows.Settings settings, OnSaleGenerator.Settings sale, List<Delivery> deliveries) {
    MinuteWindows windows = new MinuteWindows(settings, sale.partitions());
    deliveries.forEach(windows::accept);
    return windows;
  }

  private static Map<Instant, Long> runWithCrash(
      Restart restart,
      MinuteWindows.Settings settings,
      OnSaleGenerator.Settings sale,
      List<Delivery> deliveries,
      Instant crashAt) {
    MinuteWindows first = new MinuteWindows(settings, sale.partitions());
    Instant earlierAt = sale.saleStart().plus(EARLIER_CHECKPOINT);
    Instant laterAt = sale.saleStart().plus(LATER_CHECKPOINT);
    MinuteWindows.Checkpoint earlier = null;
    MinuteWindows.Checkpoint later = null;
    int crashIndex = 0;
    while (crashIndex < deliveries.size()
        && deliveries.get(crashIndex).processedAt().isBefore(crashAt)) {
      Delivery delivery = deliveries.get(crashIndex);
      // A checkpoint is taken between records, when processing time first reaches its moment.
      if (earlier == null && !delivery.processedAt().isBefore(earlierAt)) {
        earlier = first.checkpoint();
      }
      if (later == null && !delivery.processedAt().isBefore(laterAt)) {
        later = first.checkpoint();
      }
      first.accept(delivery);
      crashIndex++;
    }
    if (earlier == null || later == null) {
      throw new IllegalStateException("the crash came before both checkpoints were taken");
    }
    Map<MinuteWindows.WindowKey, MinuteWindows.Totals> dashboard = new HashMap<>(first.results());
    MinuteWindows restarted =
        switch (restart) {
          case TOGETHER -> MinuteWindows.restore(later, later);
          case OFFSETS_NEWER -> MinuteWindows.restore(earlier, later);
          case STATE_NEWER, STATE_NEWER_NO_DEDUPE -> MinuteWindows.restore(later, earlier);
          case STATE_IN_MEMORY_ONLY ->
              MinuteWindows.restore(
                  new MinuteWindows(settings, sale.partitions()).checkpoint(), later);
        };
    long[] startOffsets =
        restart == Restart.STATE_NEWER || restart == Restart.STATE_NEWER_NO_DEDUPE
            ? earlier.nextOffsets()
            : later.nextOffsets();
    for (Delivery delivery : inputAfterRestart(deliveries, crashIndex, startOffsets)) {
      restarted.accept(delivery);
    }
    dashboard.putAll(restarted.results());
    return perMinute(dashboard);
  }

  /** What the restarted consumer reads: from its offsets on, then everything not yet read. */
  private static List<Delivery> inputAfterRestart(
      List<Delivery> deliveries, int crashIndex, long[] startOffsets) {
    List<Delivery> input = new ArrayList<>();
    for (Delivery delivery : deliveries.subList(0, crashIndex)) {
      if (!delivery.isRedelivery() && delivery.offset() >= startOffsets[delivery.partition()]) {
        input.add(delivery);
      }
    }
    input.addAll(deliveries.subList(crashIndex, deliveries.size()));
    return input;
  }

  private static Map<Instant, Long> perMinute(
      Map<MinuteWindows.WindowKey, MinuteWindows.Totals> windows) {
    Map<Instant, Long> perMinute = new HashMap<>();
    windows.forEach(
        (key, totals) -> perMinute.merge(key.windowStart(), totals.confirms(), Long::sum));
    return perMinute;
  }
}
