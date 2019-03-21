package io.github.jeqo.poc.tracing.consumer;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomRebalanceListener implements ConsumerRebalanceListener {
  static final Logger LOG = LoggerFactory.getLogger(CustomRebalanceListener.class);
  final KafkaConsumer kafkaConsumer;
  // Map contains processed record(s) offset in current poll, should be cleared after each poll
  // NB! mutable
  private Map<TopicPartition, OffsetAndMetadata> processedRecordsOffsets = Collections.emptyMap();

  CustomRebalanceListener(KafkaConsumer kafkaConsumer) {
    this.kafkaConsumer = kafkaConsumer;
  }

  public void addOffset(String topic, int partition, long offset) {
    processedRecordsOffsets.put(
        new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "commit"));
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    final String partitionsRevoked =
        partitions.stream()
            .map(topicPartition -> Collections.singletonList(topicPartition.partition()).toString())
            .collect(Collectors.joining(","));
    LOG.info("Following partitions revoked: {}", partitionsRevoked);

    final String processedOffsetsBeforeRevoke =
        processedRecordsOffsets.keySet().stream()
            .map(topicPartition -> Collections.singletonList(topicPartition.partition()).toString())
            .collect(Collectors.joining(","));
    LOG.info("Following partitions will be committed: {}", processedOffsetsBeforeRevoke);

    commitProcessedRecords();
    clearProcessedRecordsOffsets();
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    final String partitionsAssigned =
        partitions.stream()
            .map(topicPartition -> Collections.singletonList(topicPartition.partition()).toString())
            .collect(Collectors.joining(","));
    LOG.info("Following partitions assigned: {}", partitionsAssigned);
  }

  // clear when partition revoke and each new poll
  void clearProcessedRecordsOffsets() {
    processedRecordsOffsets.clear();
  }

  void commitProcessedRecords() {
    processedRecordsOffsets.entrySet().stream().forEach(
        offsetInfo ->
            LOG.info("Topic partition: {}, Metadata: {}, \n last commited offset for the given partition: {}",
                offsetInfo.getKey(), offsetInfo.getValue(), kafkaConsumer.committed(offsetInfo.getKey()))
    );
    // commit sync all processed records offsets
    kafkaConsumer.commitSync(processedRecordsOffsets);
  }
}
