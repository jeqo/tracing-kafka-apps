package poc.tracing.producer.metadata;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

class MetadataPublisher {

    final Producer<String, String> kafkaProducer;
    final String topic;

    MetadataPublisher(Producer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    void publish() throws Exception {
        var record = new ProducerRecord<>(topic, "A", "B");
        kafkaProducer.send(record).get();
    }
}
