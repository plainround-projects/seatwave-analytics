package io.plainround.seatwave.analytics.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The stack programs' options: defaults, values, and every problem reported together. */
class OptionsTest {

  private static final Map<String, String> DEFAULTS = Map.of("rate", "1500", "day", "2026-11-20");

  @Test
  void usesDefaultsAndGivenValues() {
    Options options = Options.parse(new String[] {"--rate", "150"}, DEFAULTS);

    assertThat(options.integer("rate", 1, 2_000)).isEqualTo(150);
    assertThat(options.day("day")).isEqualTo(LocalDate.of(2026, 11, 20));
    assertThat(options.problems()).isEmpty();
  }

  @Test
  void reportsEveryProblemTogether() {
    Options options =
        Options.parse(new String[] {"--speed", "9", "--rate", "0", "--day", "friday"}, DEFAULTS);

    options.integer("rate", 1, 2_000);
    options.day("day");

    assertThat(options.problems())
        .containsExactly(
            "unknown option --speed (allowed: --day, --rate)",
            "--rate must be 1 to 2000: 0",
            "--day is not a day like 2026-11-20: friday");
  }

  @Test
  void reportsAMissingValueAndANumberThatIsNotOne() {
    Options options = Options.parse(new String[] {"--rate"}, DEFAULTS);

    assertThat(options.problems()).containsExactly("--rate needs a value");
    assertThat(Options.parse(new String[] {"--rate", "fast"}, DEFAULTS).integer("rate", 1, 9))
        .isEqualTo(1);
  }
}
