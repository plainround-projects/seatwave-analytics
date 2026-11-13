package io.plainround.seatwave.analytics.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * The Kafka side of the stack programs, reused from Module 28: a producer with {@code acks=all} and
 * idempotence that keeps Kafka's request and delivery timeouts, topic creation that waits until
 * every partition has a leader, and a read of a whole topic up to the end offsets it had when the
 * read started, committed records only.
 */
public final class StackClients {

  private static final Duration POLL = Duration.ofMillis(200);
  private static final short ONE_REPLICA = 1;

  /** Module 27's linger: a pass's records leave in a few batches rather than one each. */
  private static final int LINGER_MILLIS = 5;

  private StackClients() {}

  /**
   * A producer the caller owns and closes once. A send waits at most {@code timeout} for metadata
   * or buffer space; requests and deliveries keep Kafka's defaults (30 s and 120 s), because a
   * batch that expires early leaves a gap in the idempotent producer's sequence (Module 28).
   */
  public static KafkaProducer<String, String> producer(
      String bootstrap, String clientId, Duration timeout) {
    Map<String, Object> config =
        Map.ofEntries(
            Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap),
            Map.entry(ProducerConfig.CLIENT_ID_CONFIG, clientId),
            Map.entry(ProducerConfig.ACKS_CONFIG, "all"),
            Map.entry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true),
            Map.entry(ProducerConfig.LINGER_MS_CONFIG, LINGER_MILLIS),
            Map.entry(ProducerConfig.MAX_BLOCK_MS_CONFIG, Math.toIntExact(timeout.toMillis())),
            Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
            Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    return new KafkaProducer<>(config);
  }

  /** Creates each topic (name to partition count) with one replica, then waits for leaders. */
  public static void createTopics(
      String bootstrap,
      Map<String, Integer> partitionsByTopic,
      Map<String, String> topicConfig,
      Duration timeout)
      throws InterruptedException, TimeoutException {
    try (Admin admin =
        Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
      for (Map.Entry<String, Integer> topic : partitionsByTopic.entrySet()) {
        NewTopic newTopic =
            new NewTopic(topic.getKey(), topic.getValue(), ONE_REPLICA).configs(topicConfig);
        try {
          admin
              .createTopics(List.of(newTopic))
              .all()
              .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException failed) {
          if (!(failed.getCause() instanceof TopicExistsException)) {
            throw new IllegalStateException(
                "cannot create topic " + topic.getKey(), failed.getCause());
          }
        }
      }
    }
    for (Map.Entry<String, Integer> topic : partitionsByTopic.entrySet()) {
      awaitLeaders(bootstrap, topic.getKey(), topic.getValue(), timeout);
    }
  }

  /** The latest value of every key in {@code topic}, committed records only, in key order. */
  public static <V> Map<String, V> latestByKey(
      String bootstrap, String topic, Deserializer<V> values) {
    Map<String, V> latest = new TreeMap<>();
    readAll(bootstrap, topic, values).forEach(record -> latest.put(record.key(), record.value()));
    return latest;
  }

  /** How many committed records each key has in {@code topic}, in key order. */
  public static Map<String, Long> countByKey(String bootstrap, String topic) {
    Map<String, Long> counts = new TreeMap<>();
    readAll(bootstrap, topic, new StringDeserializer())
        .forEach(record -> counts.merge(record.key(), 1L, Long::sum));
    return counts;
  }

  /** A consumer with no group and read_committed isolation. The caller closes it. */
  public static <V> KafkaConsumer<String, V> reader(String bootstrap, Deserializer<V> values) {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // The stream job writes in transactions: an aborted attempt must not be read as a count.
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return new KafkaConsumer<>(config, new StringDeserializer(), values);
  }

  private static <V> List<ConsumerRecord<String, V>> readAll(
      String bootstrap, String topic, Deserializer<V> values) {
    List<ConsumerRecord<String, V>> records = new ArrayList<>();
    try (KafkaConsumer<String, V> consumer = reader(bootstrap, values)) {
      List<TopicPartition> partitions =
          consumer.partitionsFor(topic).stream()
              .map(info -> new TopicPartition(topic, info.partition()))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
      while (partitions.stream().anyMatch(part -> consumer.position(part) < endOffsets.get(part))) {
        consumer.poll(POLL).forEach(records::add);
      }
    }
    return records;
  }

  private static void awaitLeaders(String bootstrap, String topic, int partitions, Duration timeout)
      throws InterruptedException, TimeoutException {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    try (KafkaConsumer<String, String> consumer = reader(bootstrap, new StringDeserializer())) {
      while (true) {
        List<PartitionInfo> found = consumer.partitionsFor(topic, timeout);
        if (found.size() == partitions && found.stream().allMatch(info -> info.leader() != null)) {
          return;
        }
        if (System.nanoTime() > deadlineNanos) {
          throw new TimeoutException("topic " + topic + " has partitions without a leader");
        }
        Thread.sleep(POLL);
      }
    }
  }
}
