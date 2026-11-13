package io.plainround.seatwave.analytics;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The module's invariant: once the allowed lateness has passed and the nightly batch has run, the
 * stream's total for every show and minute equals the warehouse's, after the side output of records
 * that came too late is replayed; and the check fails loudly when the stream counts a duplicate.
 */
class ReconciliationTest {

  private static final int TEST_CONFIRMS_PER_SECOND = 150; // a tenth of the on-sale, same shape
  private static final LocalDate SALE_DAY = LocalDate.of(2026, 11, 20);
  private static final Duration WINDOW = Duration.ofMinutes(1);

  private final OnSaleGenerator.Settings sale =
      OnSaleGenerator.Settings.scaled(TEST_CONFIRMS_PER_SECOND);
  private final List<Delivery> deliveries = new OnSaleGenerator(sale).generate();
  private final Reconciliation reconciliation = new Reconciliation(WINDOW);

  @Test
  void streamAndBatchAgreeOnEveryMinuteOnceTheSideOutputIsReplayed() {
    MinuteWindows stream = runStream(MinuteWindows.Settings.seatWave());
    Map<MinuteWindows.WindowKey, MinuteWindows.Totals> warehouse =
        loadWarehouse().perMinute(WINDOW);

    Reconciliation.Outcome beforeCorrection =
        reconciliation.compare(stream.results(), warehouse, stream.tooLate());
    stream.applyCorrections(stream.tooLate());
    Reconciliation.Outcome afterCorrection =
        reconciliation.compare(stream.results(), warehouse, List.of());

    assertThat(stream.tooLate())
        .as("the stalled shard must produce records later than the lateness")
        .isNotEmpty();
    assertThat(beforeCorrection)
        .isInstanceOfSatisfying(
            Reconciliation.Outcome.Disagreed.class,
            disagreed -> assertThat(disagreed.isFullyExplained()).isTrue());
    assertThat(afterCorrection).isInstanceOf(Reconciliation.Outcome.Agreed.class);
    assertThat(stream.results()).isEqualTo(warehouse);
  }

  @Test
  void reconciliationReportsUnexplainedDifferencesWhenTheStreamCountsDuplicates() {
    MinuteWindows.Settings seatWave = MinuteWindows.Settings.seatWave();
    MinuteWindows careless =
        runStream(
            new MinuteWindows.Settings(
                seatWave.windowSize(),
                seatWave.watermarkDelay(),
                seatWave.allowedLateness(),
                false));

    Reconciliation.Outcome outcome =
        reconciliation.compare(
            careless.results(), loadWarehouse().perMinute(WINDOW), careless.tooLate());

    assertThat(outcome)
        .isInstanceOfSatisfying(
            Reconciliation.Outcome.Disagreed.class,
            disagreed -> assertThat(disagreed.isFullyExplained()).isFalse());
  }

  @Test
  void loadingTheSameDayTwiceLeavesTheSameRows() {
    WarehouseTable table = new WarehouseTable();
    NightlyBatch batch = new NightlyBatch();
    batch.load(SALE_DAY, deliveries, table);
    var rowsAfterFirstLoad = new HashSet<>(table.rows());

    NightlyBatch.LoadSummary second = batch.load(SALE_DAY, deliveries, table);

    assertThat(new HashSet<>(table.rows())).isEqualTo(rowsAfterFirstLoad);
    assertThat(second.count(WarehouseTable.UpsertOutcome.INSERTED)).isZero();
    assertThat(second.count(WarehouseTable.UpsertOutcome.UPDATED)).isZero();
  }

  private MinuteWindows runStream(MinuteWindows.Settings settings) {
    MinuteWindows windows = new MinuteWindows(settings, sale.partitions());
    deliveries.forEach(windows::accept);
    windows.endOfInput(); // the check runs after the day has closed and the lateness has passed
    return windows;
  }

  private WarehouseTable loadWarehouse() {
    WarehouseTable table = new WarehouseTable();
    new NightlyBatch().load(SALE_DAY, deliveries, table);
    return table;
  }
}
