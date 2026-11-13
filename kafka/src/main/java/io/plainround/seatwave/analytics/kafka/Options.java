package io.plainround.seatwave.analytics.kafka;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * The command-line options of the stack programs: {@code --name value} pairs, each name from the
 * program's allow-list with a default. Every problem (an unknown name, a missing value, a value out
 * of range or of the wrong form) is collected, so one run reports them all together.
 *
 * <p>Not thread-safe: built and read on the main thread.
 */
public final class Options {

  private final Map<String, String> values;
  private final List<String> problems = new ArrayList<>();

  private Options(Map<String, String> values, List<String> problems) {
    this.values = values;
    this.problems.addAll(problems);
  }

  /** Parses {@code arguments} against {@code defaults}, whose keys are the allowed names. */
  public static Options parse(String[] arguments, Map<String, String> defaults) {
    Objects.requireNonNull(arguments, "arguments");
    Map<String, String> values = new LinkedHashMap<>(defaults);
    List<String> problems = new ArrayList<>();
    Set<String> allowed = defaults.keySet();
    for (int index = 0; index < arguments.length; index += 2) {
      String name = arguments[index];
      if (!name.startsWith("--") || !allowed.contains(name.substring(2))) {
        problems.add(
            "unknown option "
                + name
                + " (allowed: --"
                + String.join(", --", new TreeSet<>(allowed))
                + ")");
        continue;
      }
      if (index + 1 >= arguments.length) {
        problems.add(name + " needs a value");
        continue;
      }
      values.put(name.substring(2), arguments[index + 1]);
    }
    return new Options(values, problems);
  }

  /** A text value; problems if it is blank. */
  public String text(String name) {
    String value = values.get(name);
    if (value == null || value.isBlank()) {
      problems.add("--" + name + " must not be empty");
      return "";
    }
    return value;
  }

  /** A whole number from {@code min} to {@code max}. */
  public int integer(String name, int min, int max) {
    try {
      int value = Integer.parseInt(text(name));
      if (value < min || value > max) {
        problems.add("--" + name + " must be " + min + " to " + max + ": " + value);
      }
      return value;
    } catch (NumberFormatException notNumber) {
      problems.add("--" + name + " is not a whole number: " + values.get(name));
      return min;
    }
  }

  /** A folder or file path. */
  public Path path(String name) {
    return Path.of(text(name));
  }

  /** A UTC day such as {@code 2026-11-20}. */
  public LocalDate day(String name) {
    try {
      return LocalDate.parse(text(name));
    } catch (DateTimeParseException notDay) {
      problems.add("--" + name + " is not a day like 2026-11-20: " + values.get(name));
      return LocalDate.EPOCH;
    }
  }

  /** Every problem found so far; empty means the options can be used. */
  public List<String> problems() {
    return List.copyOf(problems);
  }
}
