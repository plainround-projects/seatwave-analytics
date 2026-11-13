package io.plainround.seatwave.analytics;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The stream job's operator: tickets confirmed (and canceled) per show per minute of event time, in
 * tumbling windows, with a watermark that trails the largest event time seen by a fixed delay, an
 * allowed lateness during which late records still update a window, and a side output for records
 * later than that. Optionally drops repeats of a message id inside a window.
 *
 * <p>Thread-safety: not thread-safe; one job task owns one instance, as a stream task owns its
 * state store.
 */
final class MinuteWindows {

  /**
   * The window policy.
   *
   * @param windowSize the tumbling window length
   * @param watermarkDelay how far the watermark trails the largest event time seen
   * @param allowedLateness how long after the watermark passes a window's end it still accepts
   * @param isDeduplicating whether repeats of a message id inside a window are dropped
   */
  record Settings(
      Duration windowSize,
      Duration watermarkDelay,
      Duration allowedLateness,
      boolean isDeduplicating) {

    Settings {
      Objects.requireNonNull(windowSize, "windowSize");
      Objects.requireNonNull(watermarkDelay, "watermarkDelay");
      Objects.requireNonNull(allowedLateness, "allowedLateness");
      if (windowSize.isNegative() || windowSize.isZero()) {
        throw new IllegalArgumentException("windowSize must be positive");
      }
      if (watermarkDelay.isNegative() || allowedLateness.isNegative()) {
        throw new IllegalArgumentException(
            "watermarkDelay and allowedLateness must not be negative");
      }
    }

    /** SeatWave's choice (Section 7): 1-minute windows, a 10 s watermark, 2 minutes of lateness. */
    static Settings seatWave() {
      return new Settings(
          Duration.ofMinutes(1), Duration.ofSeconds(10), Duration.ofMinutes(2), true);
    }
  }

  /** One window: a show and the start of its minute. */
  record WindowKey(long eventId, Instant windowStart) {
    WindowKey {
      Objects.requireNonNull(windowStart, "windowStart");
    }
  }

  /** What a window counted. */
  record Totals(long confirms, long cancels) {
    static final Totals ZERO = new Totals(0, 0);

    Totals plus(Totals other) {
      return new Totals(
          Math.addExact(confirms, other.confirms), Math.addExact(cancels, other.cancels));
    }
  }

  /** Counters a job would export as metrics. */
  record Stats(
      long accepted,
      long lateAccepted,
      long tooLate,
      long duplicatesDropped,
      long idsHeldNow,
      long maxIdsHeld,
      int windowsOpen) {}

  /** A copy of everything the operator holds, taken at one moment; restoring copies it again. */
  static final class Checkpoint {
    private final MinuteWindows frozen;

    private Checkpoint(MinuteWindows frozen) {
      this.frozen = frozen;
    }

    long[] nextOffsets() {
      return frozen.nextOffsets.clone();
    }
  }

  private static final class OpenWindow {
    private long confirms;
    private long cancels;
    private boolean isFired;
    private final Set<UUID> seenMessageIds = new HashSet<>();

    private OpenWindow copy() {
      OpenWindow copy = new OpenWindow();
      copy.confirms = confirms;
      copy.cancels = cancels;
      copy.isFired = isFired;
      copy.seenMessageIds.addAll(seenMessageIds);
      return copy;
    }

    private Totals totals() {
      return new Totals(confirms, cancels);
    }
  }

  private final Settings settings;
  private final NavigableMap<Instant, Map<Long, OpenWindow>> openByStart = new TreeMap<>();
  private final Map<WindowKey, Totals> closed = new HashMap<>();
  private final Map<WindowKey, Totals> firstResults = new HashMap<>();
  private final List<Delivery> tooLate = new ArrayList<>();
  private final long[] nextOffsets;
  private Instant maxEventTime = Instant.MIN;
  private Instant firedThrough = Instant.MIN;
  private long accepted;
  private long lateAccepted;
  private long duplicatesDropped;
  private long idsHeld;
  private long maxIdsHeld;

  MinuteWindows(Settings settings, int partitions) {
    this.settings = Objects.requireNonNull(settings, "settings");
    if (partitions < 1) {
      throw new IllegalArgumentException("partitions must be positive: " + partitions);
    }
    this.nextOffsets = new long[partitions];
  }

  /**
   * A new operator holding the state of {@code state} and the read positions of {@code offsets}.
   */
  static MinuteWindows restore(Checkpoint state, Checkpoint offsets) {
    MinuteWindows restored = state.frozen.copy();
    System.arraycopy(
        offsets.frozen.nextOffsets, 0, restored.nextOffsets, 0, restored.nextOffsets.length);
    return restored;
  }

  /** Counts one record, or sends it to the side output if its window has closed. */
  void accept(Delivery delivery) {
    int partition = delivery.partition();
    nextOffsets[partition] = Math.max(nextOffsets[partition], delivery.offset() + 1);
    BookingEvent event = delivery.event();
    Instant windowStart = windowStartOf(event.occurredAt());
    if (!windowStart
        .plus(settings.windowSize())
        .plus(settings.allowedLateness())
        .isAfter(watermark())) {
      tooLate.add(delivery); // the window's state is gone: only a correction can count it now
      return;
    }
    OpenWindow window =
        openByStart
            .computeIfAbsent(windowStart, start -> new HashMap<>())
            .computeIfAbsent(event.eventId(), eventId -> newWindow(windowStart));
    if (settings.isDeduplicating() && !window.seenMessageIds.add(event.messageId())) {
      duplicatesDropped++;
      return;
    }
    if (settings.isDeduplicating()) {
      idsHeld++;
      maxIdsHeld = Math.max(maxIdsHeld, idsHeld);
    }
    if (event.isConfirm()) {
      window.confirms++;
    } else {
      window.cancels++;
    }
    accepted++;
    if (window.isFired) {
      lateAccepted++; // a late firing: the dashboard's number for this minute changes
    }
    advanceWatermark(event.occurredAt());
  }

  /** The input is bounded and has ended: the watermark jumps past every window. */
  void endOfInput() {
    fireUpTo(Instant.MAX);
    closeUpTo(Instant.MAX);
  }

  /** Adds side-output records to closed windows, once per message id; returns how many counted. */
  long applyCorrections(List<Delivery> lateRecords) {
    Set<UUID> applied = new HashSet<>();
    long counted = 0;
    for (Delivery delivery : lateRecords) {
      BookingEvent event = delivery.event();
      if (!applied.add(event.messageId())) {
        continue;
      }
      WindowKey key = new WindowKey(event.eventId(), windowStartOf(event.occurredAt()));
      Totals one = event.isConfirm() ? new Totals(1, 0) : new Totals(0, 1);
      closed.merge(key, one, Totals::plus);
      counted++;
    }
    return counted;
  }

  Checkpoint checkpoint() {
    return new Checkpoint(copy());
  }

  /** The latest totals of every window, open or closed. */
  Map<WindowKey, Totals> results() {
    Map<WindowKey, Totals> results = new HashMap<>(closed);
    openByStart.forEach((start, windows) ->
        windows.forEach(
            (eventId, window) -> results.put(new WindowKey(eventId, start), window.totals())));
    return results;
  }

  /** What each window showed when the watermark first passed its end (the provisional number). */
  Map<WindowKey, Totals> firstResults() {
    return Map.copyOf(firstResults);
  }

  List<Delivery> tooLate() {
    return List.copyOf(tooLate);
  }

  Instant watermark() {
    return maxEventTime.equals(Instant.MIN)
        ? Instant.MIN
        : maxEventTime.minus(settings.watermarkDelay());
  }

  Stats stats() {
    int windowsOpen = openByStart.values().stream().mapToInt(Map::size).sum();
    return new Stats(
        accepted,
        lateAccepted,
        tooLate.size(),
        duplicatesDropped,
        idsHeld,
        maxIdsHeld,
        windowsOpen);
  }

  Instant windowStartOf(Instant eventTime) {
    long sizeMillis = settings.windowSize().toMillis();
    return Instant.ofEpochMilli(Math.floorDiv(eventTime.toEpochMilli(), sizeMillis) * sizeMillis);
  }

  private OpenWindow newWindow(Instant windowStart) {
    OpenWindow window = new OpenWindow();
    // A window first seen after the watermark passed its end has, in effect, fired with 0.
    window.isFired = windowStart.isBefore(firedThrough);
    return window;
  }

  private void advanceWatermark(Instant eventTime) {
    if (!eventTime.isAfter(maxEventTime)) {
      return; // an out-of-order record never moves the watermark back
    }
    maxEventTime = eventTime;
    fireUpTo(watermark());
    closeUpTo(watermark());
  }

  private void fireUpTo(Instant watermarkNow) {
    for (Map.Entry<Instant, Map<Long, OpenWindow>> entry :
        openByStart.tailMap(firedThrough, true).entrySet()) {
      Instant windowEnd = entry.getKey().plus(settings.windowSize());
      if (windowEnd.isAfter(watermarkNow)) {
        break;
      }
      entry
          .getValue()
          .forEach((eventId, window) -> {
            if (!window.isFired) {
              window.isFired = true;
              firstResults.put(new WindowKey(eventId, entry.getKey()), window.totals());
            }
          });
      firedThrough = windowEnd;
    }
  }

  private void closeUpTo(Instant watermarkNow) {
    while (!openByStart.isEmpty()) {
      Instant start = openByStart.firstKey();
      boolean isPastLateness =
          watermarkNow.equals(Instant.MAX)
              || !start
                  .plus(settings.windowSize())
                  .plus(settings.allowedLateness())
                  .isAfter(watermarkNow);
      if (!isPastLateness) {
        return;
      }
      openByStart
          .pollFirstEntry()
          .getValue()
          .forEach((eventId, window) -> {
            closed.put(new WindowKey(eventId, start), window.totals());
            idsHeld -= window.seenMessageIds.size(); // the ids leave memory with the window
          });
    }
  }

  private MinuteWindows copy() {
    MinuteWindows copy = new MinuteWindows(settings, nextOffsets.length);
    openByStart.forEach((start, windows) -> {
      Map<Long, OpenWindow> copied = new HashMap<>();
      windows.forEach((eventId, window) -> copied.put(eventId, window.copy()));
      copy.openByStart.put(start, copied);
    });
    copy.closed.putAll(closed);
    copy.firstResults.putAll(firstResults);
    copy.tooLate.addAll(tooLate);
    System.arraycopy(nextOffsets, 0, copy.nextOffsets, 0, nextOffsets.length);
    copy.maxEventTime = maxEventTime;
    copy.firedThrough = firedThrough;
    copy.accepted = accepted;
    copy.lateAccepted = lateAccepted;
    copy.duplicatesDropped = duplicatesDropped;
    copy.idsHeld = idsHeld;
    copy.maxIdsHeld = maxIdsHeld;
    return copy;
  }
}
