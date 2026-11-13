package io.plainround.seatwave.analytics;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * The nightly batch loads the day into {@code fact_bookings} twice, the reconciliation compares the
 * stream's per-show, per-minute totals with it, the two corrections (side-output replay and a
 * bounded replay of the day) make them agree, net sales are compared, and a stream without dedupe
 * is caught by the same check.
 */
final class ReconcileCli {

  private static final DateTimeFormatter MINUTE =
      DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT).withZone(ZoneOffset.UTC);
  private static final LocalDate SALE_DAY = LocalDate.of(2026, 11, 20);
  private static final Duration WINDOW = Duration.ofMinutes(1);
  private static final Duration HELD_TO_END_OF_DAY = Duration.ofDays(1);
  private static final int SALE_MINUTES = 15;

  private ReconcileCli() {}

  public static void main(String[] arguments) {
    OnSaleGenerator.Settings sale = OnSaleGenerator.Settings.nationalOnSale();
    List<Delivery> deliveries = new OnSaleGenerator(sale).generate();
    WarehouseTable table = new WarehouseTable();
    NightlyBatch batch = new NightlyBatch();
    System.out.println(
        "Part 1. The nightly batch: the archive of "
            + SALE_DAY
            + " (written at least once) into fact_bookings");
    printLoad("first load ", batch.load(SALE_DAY, deliveries, table));
    printLoad("second load", batch.load(SALE_DAY, deliveries, table));
    Map<MinuteWindows.WindowKey, MinuteWindows.Totals> warehouse = table.perMinute(WINDOW);

    MinuteWindows live = run(MinuteWindows.Settings.seatWave(), sale, deliveries);
    Reconciliation reconciliation = new Reconciliation(WINDOW);
    System.out.println();
    System.out.println(
        "Part 2. Reconciliation: the live stream's closed windows against the warehouse");
    print(reconciliation.compare(live.results(), warehouse, live.tooLate()));

    System.out.println();
    System.out.println("Part 3. Two ways to correct the stream, then the same check");
    long applied = live.applyCorrections(live.tooLate());
    System.out.println(
        String.format(
            Locale.ROOT,
            "  side-output replay: %,d late records added to their closed windows",
            applied));
    print(reconciliation.compare(live.results(), warehouse, List.of()));
    List<Delivery> logOrder =
        deliveries.stream()
            .filter(delivery -> !delivery.isRedelivery())
            .sorted(Comparator.comparing(Delivery::appendedAt))
            .toList();
    MinuteWindows sameSettings = run(MinuteWindows.Settings.seatWave(), sale, logOrder);
    System.out.println("  replay of the day from offset 0 with the live watermark and lateness:");
    print(reconciliation.compare(sameSettings.results(), warehouse, sameSettings.tooLate()));
    MinuteWindows bounded =
        run(
            new MinuteWindows.Settings(WINDOW, HELD_TO_END_OF_DAY, Duration.ZERO, true),
            sale,
            logOrder);
    System.out.println(
        "  replay of the day as a bounded input (watermark held until the input ends):");
    print(reconciliation.compare(bounded.results(), warehouse, bounded.tooLate()));

    System.out.println();
    printNet(live, table);

    System.out.println();
    System.out.println("Part 5. A stream job without dedupe, checked the same way");
    MinuteWindows.Settings base = MinuteWindows.Settings.seatWave();
    MinuteWindows careless =
        run(
            new MinuteWindows.Settings(
                base.windowSize(), base.watermarkDelay(), base.allowedLateness(), false),
            sale,
            deliveries);
    print(reconciliation.compare(careless.results(), warehouse, careless.tooLate()));
  }

  private static MinuteWindows run(
      MinuteWindows.Settings settings, OnSaleGenerator.Settings sale, List<Delivery> input) {
    MinuteWindows windows = new MinuteWindows(settings, sale.partitions());
    input.forEach(windows::accept);
    // The reconciliation runs after the day has closed: the idle stream's watermark is advanced.
    windows.endOfInput();
    return windows;
  }

  private static void printLoad(String label, NightlyBatch.LoadSummary summary) {
    System.out.println(
        String.format(
            Locale.ROOT,
            "  %s: %,d records read; inserted %,d, updated %,d, ignored as same version %,d, older"
                + " %,d, no confirm %,d; rows %,d",
            label,
            summary.recordsRead(),
            summary.count(WarehouseTable.UpsertOutcome.INSERTED),
            summary.count(WarehouseTable.UpsertOutcome.UPDATED),
            summary.count(WarehouseTable.UpsertOutcome.IGNORED_SAME_VERSION),
            summary.count(WarehouseTable.UpsertOutcome.IGNORED_OLDER_VERSION),
            summary.count(WarehouseTable.UpsertOutcome.IGNORED_NO_CONFIRM),
            summary.rowsAfter()));
  }

  private static void print(Reconciliation.Outcome outcome) {
    switch (outcome) {
      case Reconciliation.Outcome.Agreed agreed ->
          System.out.println(
              String.format(
                  Locale.ROOT,
                  "    AGREED: %,d windows (show and minute) compared, 0 differ",
                  agreed.windowsCompared()));
      case Reconciliation.Outcome.Disagreed disagreed -> {
        System.out.println(
            String.format(
                Locale.ROOT,
                "    DISAGREED: %,d windows compared, %,d differ; fully explained by the side"
                    + " output: %s",
                disagreed.windowsCompared(),
                disagreed.differences().size(),
                disagreed.isFullyExplained() ? "yes" : "NO"));
        System.out.println(
            "    minute  windows  stream confirms/cancels  warehouse  explained (too late) "
                + " unexplained");
        Map<Instant, long[]> byMinute = new TreeMap<>();
        for (Reconciliation.MinuteDifference difference : disagreed.differences()) {
          long[] sums =
              byMinute.computeIfAbsent(difference.key().windowStart(), start -> new long[9]);
          MinuteWindows.Totals unexplained = difference.unexplained();
          long[] values = {
            1,
            difference.stream().confirms(),
            difference.stream().cancels(),
            difference.batch().confirms(),
            difference.batch().cancels(),
            difference.explainedByTooLate().confirms(),
            difference.explainedByTooLate().cancels(),
            unexplained.confirms(),
            unexplained.cancels()
          };
          for (int column = 0; column < values.length; column++) {
            sums[column] += values[column];
          }
        }
        byMinute.forEach((minute, sums) ->
            System.out.println(
                String.format(
                    Locale.ROOT,
                    "    %s  %7d  %,10d / %,5d   %,7d / %,4d   %,6d / %,4d        %+,5d / %+,d",
                    MINUTE.format(minute),
                    sums[0],
                    sums[1],
                    sums[2],
                    sums[3],
                    sums[4],
                    sums[5],
                    sums[6],
                    sums[7],
                    sums[8])));
      }
    }
  }

  private static void printNet(MinuteWindows live, WarehouseTable table) {
    Map<Instant, Long> streamNet = new HashMap<>();
    live.results()
        .forEach((key, totals) ->
            streamNet.merge(key.windowStart(), totals.confirms() - totals.cancels(), Long::sum));
    Map<Instant, Long> stillConfirmed = table.stillConfirmedByMinute(WINDOW);
    Instant saleStart = SALE_DAY.atTime(19, 0).toInstant(ZoneOffset.UTC);
    Instant saleEnd = saleStart.plus(WINDOW.multipliedBy(SALE_MINUTES));
    System.out.println(
        "Part 4. Net sales: the stream's confirms minus cancels by event minute, the warehouse's"
            + " still-confirmed by confirm minute");
    System.out.println("    minute   stream net   warehouse net");
    for (int minute = 0; minute < SALE_MINUTES; minute += 2) {
      Instant start = saleStart.plus(WINDOW.multipliedBy(minute));
      System.out.println(
          String.format(
              Locale.ROOT,
              "    %s  %,11d  %,14d",
              MINUTE.format(start),
              streamNet.getOrDefault(start, 0L),
              stillConfirmed.getOrDefault(start, 0L)));
    }
    long streamAfter =
        streamNet.entrySet().stream()
            .filter(entry -> !entry.getKey().isBefore(saleEnd))
            .mapToLong(Map.Entry::getValue)
            .sum();
    long tableAfter =
        stillConfirmed.entrySet().stream()
            .filter(entry -> !entry.getKey().isBefore(saleEnd))
            .mapToLong(Map.Entry::getValue)
            .sum();
    System.out.println(
        String.format(
            Locale.ROOT,
            "    after %s  %,9d  %,14d",
            MINUTE.format(saleEnd),
            streamAfter,
            tableAfter));
    System.out.println(
        String.format(
            Locale.ROOT,
            "    day      %,11d  %,14d",
            streamNet.values().stream().mapToLong(Long::longValue).sum(),
            stillConfirmed.values().stream().mapToLong(Long::longValue).sum()));
  }
}
