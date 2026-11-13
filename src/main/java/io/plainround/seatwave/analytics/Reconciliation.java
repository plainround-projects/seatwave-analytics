package io.plainround.seatwave.analytics;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * The job {@code sales-reconciliation}: compares the stream's per-show, per-minute totals with the
 * warehouse's after the nightly batch, and explains each difference by the stream's side output of
 * records that arrived after the allowed lateness. A minute may be marked final on the dashboard
 * only through {@link Outcome.Agreed}.
 *
 * <p>Thread-safety: stateless; safe to share.
 */
final class Reconciliation {

  /** One window whose totals differ. */
  record MinuteDifference(
      MinuteWindows.WindowKey key,
      MinuteWindows.Totals stream,
      MinuteWindows.Totals batch,
      MinuteWindows.Totals explainedByTooLate) {

    MinuteDifference {
      Objects.requireNonNull(key, "key");
      Objects.requireNonNull(stream, "stream");
      Objects.requireNonNull(batch, "batch");
      Objects.requireNonNull(explainedByTooLate, "explainedByTooLate");
    }

    /** What the side output does not account for: a bug, a duplicate, or a lost record. */
    MinuteWindows.Totals unexplained() {
      return new MinuteWindows.Totals(
          batch.confirms() - stream.confirms() - explainedByTooLate.confirms(),
          batch.cancels() - stream.cancels() - explainedByTooLate.cancels());
    }

    boolean isExplained() {
      return unexplained().equals(MinuteWindows.Totals.ZERO);
    }
  }

  /** The result of one comparison. */
  sealed interface Outcome {
    /** Every window has the same totals on both sides: the minutes can be marked final. */
    record Agreed(int windowsCompared) implements Outcome {}

    /** Some windows differ; each difference is listed in window order. */
    record Disagreed(int windowsCompared, List<MinuteDifference> differences) implements Outcome {
      public Disagreed {
        differences = List.copyOf(differences);
      }

      boolean isFullyExplained() {
        return differences.stream().allMatch(MinuteDifference::isExplained);
      }
    }
  }

  private final Duration windowSize;

  Reconciliation(Duration windowSize) {
    this.windowSize = Objects.requireNonNull(windowSize, "windowSize");
  }

  /** Compares every window present on either side. */
  Outcome compare(
      Map<MinuteWindows.WindowKey, MinuteWindows.Totals> stream,
      Map<MinuteWindows.WindowKey, MinuteWindows.Totals> batch,
      List<Delivery> tooLate) {
    Objects.requireNonNull(stream, "stream");
    Objects.requireNonNull(batch, "batch");
    Objects.requireNonNull(tooLate, "tooLate");
    Map<MinuteWindows.WindowKey, MinuteWindows.Totals> explained = totalsOf(tooLate);
    Comparator<MinuteWindows.WindowKey> windowOrder =
        Comparator.comparing(MinuteWindows.WindowKey::windowStart)
            .thenComparingLong(MinuteWindows.WindowKey::eventId);
    Set<MinuteWindows.WindowKey> keys = new TreeSet<>(windowOrder);
    keys.addAll(stream.keySet());
    keys.addAll(batch.keySet());
    List<MinuteDifference> differences = new ArrayList<>();
    for (MinuteWindows.WindowKey key : keys) {
      MinuteWindows.Totals streamTotals = stream.getOrDefault(key, MinuteWindows.Totals.ZERO);
      MinuteWindows.Totals batchTotals = batch.getOrDefault(key, MinuteWindows.Totals.ZERO);
      if (!streamTotals.equals(batchTotals)) {
        differences.add(
            new MinuteDifference(
                key,
                streamTotals,
                batchTotals,
                explained.getOrDefault(key, MinuteWindows.Totals.ZERO)));
      }
    }
    return differences.isEmpty()
        ? new Outcome.Agreed(keys.size())
        : new Outcome.Disagreed(keys.size(), differences);
  }

  /** The side output's records per window, each message id once. */
  private Map<MinuteWindows.WindowKey, MinuteWindows.Totals> totalsOf(List<Delivery> records) {
    Map<MinuteWindows.WindowKey, MinuteWindows.Totals> totals = new HashMap<>();
    Set<UUID> seen = new HashSet<>();
    for (Delivery delivery : records) {
      BookingEvent event = delivery.event();
      if (!seen.add(event.messageId())) {
        continue;
      }
      long sizeMillis = windowSize.toMillis();
      Instant start =
          Instant.ofEpochMilli(
              Math.floorDiv(event.occurredAt().toEpochMilli(), sizeMillis) * sizeMillis);
      MinuteWindows.Totals one =
          event.isConfirm() ? new MinuteWindows.Totals(1, 0) : new MinuteWindows.Totals(0, 1);
      totals.merge(
          new MinuteWindows.WindowKey(event.eventId(), start), one, MinuteWindows.Totals::plus);
    }
    return totals;
  }
}
