package io.plainround.seatwave.analytics.warehouse;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The real path's reconciliation: agreement, a difference the late topic explains, and one it
 * cannot.
 */
class StreamBatchCheckTest {

  @Test
  void agreesWhenEveryWindowMatches() {
    StreamBatchCheck.Report report =
        StreamBatchCheck.compare(Map.of("7@19:07", 90L), Map.of(), Map.of("7@19:07", 90L));

    assertThat(report.isAgreed()).isTrue();
    assertThat(report.differingBefore()).isZero();
  }

  @Test
  void explainsAShortfallThatTheLateTopicHolds() {
    StreamBatchCheck.Report report =
        StreamBatchCheck.compare(
            Map.of("7@19:07", 88L), Map.of("7@19:07", 2L), Map.of("7@19:07", 90L));

    assertThat(report.isAgreed()).isTrue();
    assertThat(report.differingBefore()).isEqualTo(1);
    assertThat(report.explainedByLate()).isEqualTo(1);
    assertThat(report.lateRecords()).isEqualTo(2);
  }

  @Test
  void reportsAWindowCountedTwiceOrMissingOnOneSide() {
    StreamBatchCheck.Report report =
        StreamBatchCheck.compare(
            Map.of("7@19:07", 91L), Map.of(), Map.of("7@19:07", 90L, "7@19:08", 5L));

    assertThat(report.isAgreed()).isFalse();
    assertThat(report.overcounted()).isEqualTo(1);
    assertThat(report.unexplained())
        .containsExactly(
            "7@19:07: stream 91 + late 0 != warehouse 90",
            "7@19:08: stream 0 + late 0 != warehouse 5");
  }
}
