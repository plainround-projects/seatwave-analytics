package io.plainround.seatwave.analytics.warehouse;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * The job {@code sales-reconciliation} on the real path: compares the stream's confirms per window
 * (the latest value of each key in {@code seatwave.analytics.sales-per-minute}) with the
 * warehouse's ({@code sales_per_minute.confirms}), before and after the correction that adds the
 * confirms of {@code seatwave.analytics.late-bookings} to their windows. Only {@link
 * Report#isAgreed()} may mark the minutes final.
 *
 * <p>Thread-safety: stateless.
 */
public final class StreamBatchCheck {

  private StreamBatchCheck() {}

  /**
   * One comparison.
   *
   * @param windowsCompared keys on either side
   * @param differingBefore windows whose stream count differs from the warehouse's
   * @param lateRecords confirms in the late topic
   * @param explainedByLate differing windows that the late confirms make equal
   * @param overcounted windows where the stream counted more than the warehouse holds: a confirm
   *     counted twice, which no correction can explain
   * @param unexplained the differing windows that remain after the correction
   */
  public record Report(
      int windowsCompared,
      int differingBefore,
      long lateRecords,
      int explainedByLate,
      int overcounted,
      List<String> unexplained) {

    public Report {
      unexplained = List.copyOf(unexplained);
    }

    /** True only when every window agrees after the correction. */
    public boolean isAgreed() {
      return unexplained.isEmpty();
    }
  }

  /** Compares every window key on either side; a key missing on one side counts as 0 there. */
  public static Report compare(
      Map<String, Long> stream, Map<String, Long> late, Map<String, Long> warehouse) {
    Objects.requireNonNull(stream, "stream");
    Objects.requireNonNull(late, "late");
    Objects.requireNonNull(warehouse, "warehouse");
    TreeSet<String> keys = new TreeSet<>(stream.keySet());
    keys.addAll(warehouse.keySet());
    keys.addAll(late.keySet());
    int differing = 0;
    int explained = 0;
    int overcounted = 0;
    List<String> unexplained = new java.util.ArrayList<>();
    for (String key : keys) {
      long streamCount = stream.getOrDefault(key, 0L);
      long lateCount = late.getOrDefault(key, 0L);
      long warehouseCount = warehouse.getOrDefault(key, 0L);
      if (streamCount > warehouseCount) {
        overcounted++;
      }
      if (streamCount == warehouseCount && lateCount == 0) {
        continue;
      }
      differing += streamCount == warehouseCount ? 0 : 1;
      if (streamCount + lateCount == warehouseCount) {
        explained++;
      } else {
        unexplained.add(
            key
                + ": stream "
                + streamCount
                + " + late "
                + lateCount
                + " != warehouse "
                + warehouseCount);
      }
    }
    long lateRecords = late.values().stream().mapToLong(Long::longValue).sum();
    return new Report(keys.size(), differing, lateRecords, explained, overcounted, unexplained);
  }
}
