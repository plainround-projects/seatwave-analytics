package io.plainround.seatwave.analytics.kafka;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.KafkaStreams;

/**
 * Runs the stream job {@code analytics-sales} until it is stopped: a TERM signal closes it cleanly,
 * a KILL (the crash drill) does not, and exactly-once processing makes both safe. Prints its
 * process id first, so a drill can stop this process and no other.
 *
 * <p>Usage: {@code --bootstrap 127.0.0.1:23192 --state build/streams-state}
 */
public final class StreamCli {

  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);

  private StreamCli() {}

  public static void main(String[] arguments) throws InterruptedException {
    Options options =
        Options.parse(
            arguments, Map.of("bootstrap", "127.0.0.1:23192", "state", "build/streams-state"));
    String bootstrap = options.text("bootstrap");
    var stateFolder = options.path("state");
    if (!options.problems().isEmpty()) {
      options.problems().forEach(System.err::println);
      System.exit(2);
    }
    CountDownLatch stopped = new CountDownLatch(1);
    KafkaStreams streams =
        new KafkaStreams(
            SalesPerMinuteTopology.build(),
            SalesPerMinuteTopology.settings(bootstrap, stateFolder));
    streams.setStateListener((newState, oldState) -> {
      System.out.println("state " + oldState + " -> " + newState);
      if (newState == KafkaStreams.State.ERROR || newState == KafkaStreams.State.NOT_RUNNING) {
        stopped.countDown();
      }
    });
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(() -> {
              streams.close(CLOSE_TIMEOUT);
              stopped.countDown();
            }));
    System.out.println("pid " + ProcessHandle.current().pid() + " analytics-sales starting");
    streams.start();
    stopped.await();
    streams.close(CLOSE_TIMEOUT);
    System.exit(streams.state() == KafkaStreams.State.ERROR ? 1 : 0);
  }
}
