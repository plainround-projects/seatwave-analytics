package io.plainround.seatwave.analytics.warehouse;

import io.plainround.seatwave.analytics.kafka.Options;
import io.plainround.seatwave.analytics.kafka.SalesPerMinuteTopology;
import io.plainround.seatwave.analytics.kafka.StackClients;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.common.serialization.LongDeserializer;

/**
 * The job {@code sales-reconciliation} (scheduled at 02:05, after the load): reads the stream's
 * latest count of every window and the late topic from Kafka (committed records only) and the
 * warehouse's view, and prints the comparison before and after the late-record correction. Exits 1
 * when a difference remains after the correction: the alert of Section 10.
 *
 * <p>Usage: {@code --bootstrap 127.0.0.1:23192 --db build/warehouse/seatwave.duckdb --lake-parent
 * .}
 */
public final class SalesReconciliationCli {

  private static final int SHOWN = 5;

  private SalesReconciliationCli() {}

  public static void main(String[] arguments) throws SQLException, IOException {
    Options options =
        Options.parse(
            arguments,
            Map.of(
                "bootstrap", "127.0.0.1:23192",
                "db", "build/warehouse/seatwave.duckdb",
                "lake-parent", "."));
    String bootstrap = options.text("bootstrap");
    Path database = options.path("db");
    Path lakeParent = options.path("lake-parent");
    if (!options.problems().isEmpty()) {
      options.problems().forEach(System.err::println);
      System.exit(2);
    }
    Map<String, Long> stream =
        StackClients.latestByKey(
            bootstrap, SalesPerMinuteTopology.OUTPUT_TOPIC, new LongDeserializer());
    Map<String, Long> late = StackClients.countByKey(bootstrap, SalesPerMinuteTopology.LATE_TOPIC);
    long deadLettered =
        StackClients.countByKey(bootstrap, SalesPerMinuteTopology.DEAD_LETTER_TOPIC)
            .values()
            .stream()
            .mapToLong(Long::longValue)
            .sum();
    Map<String, Long> warehouse;
    try (Warehouse opened = Warehouse.open(database, lakeParent)) {
      warehouse = opened.confirmsPerMinute();
    }
    StreamBatchCheck.Report report = StreamBatchCheck.compare(stream, late, warehouse);
    long streamTotal = stream.values().stream().mapToLong(Long::longValue).sum();
    long warehouseTotal = warehouse.values().stream().mapToLong(Long::longValue).sum();
    System.out.printf(
        Locale.ROOT,
        "sales-reconciliation: %,d windows (show and minute) compared; stream %,d confirms,"
            + " late topic %,d, warehouse %,d%n",
        report.windowsCompared(),
        streamTotal,
        report.lateRecords(),
        warehouseTotal);
    System.out.printf(
        Locale.ROOT,
        "  before the correction: %,d windows differ by %,d confirms; %,d explained by the late"
            + " topic; %,d windows where the stream counted more than the warehouse%n",
        report.differingBefore(),
        warehouseTotal - streamTotal,
        report.explainedByLate(),
        report.overcounted());
    System.out.printf(
        Locale.ROOT,
        "  after the correction:  %s%n",
        report.isAgreed()
            ? "AGREED, 0 windows differ"
            : "DISAGREED, " + report.unexplained().size() + " windows differ");
    Map<String, Long> lateByMinute = new TreeMap<>();
    late.forEach((key, confirms) -> lateByMinute.merge(minuteOf(key), confirms, Long::sum));
    lateByMinute.forEach((minute, confirms) ->
        System.out.printf(Locale.ROOT, "    late confirms of minute %s: %,d%n", minute, confirms));
    System.out.printf(
        Locale.ROOT,
        "  dead-letter topic %s: %,d records the reader could not use%n",
        SalesPerMinuteTopology.DEAD_LETTER_TOPIC,
        deadLettered);
    report.unexplained().stream().limit(SHOWN).forEach(line -> System.out.println("    " + line));
    if (!report.isAgreed()) {
      System.exit(1);
    }
  }

  /** The minute of a window key {@code <event_id>@<minute start>}, as HH:MM. */
  private static String minuteOf(String windowKey) {
    String start = windowKey.substring(windowKey.indexOf('@') + 1);
    return start.substring(start.indexOf('T') + 1, start.indexOf('T') + "HH:MM".length() + 1);
  }
}
