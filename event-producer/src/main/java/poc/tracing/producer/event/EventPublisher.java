package poc.tracing.producer.event;

import brave.Tracing;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

class EventPublisher {

  final String topic;

  final Producer<String, String> kafkaProducer;

  EventPublisher(Producer<String, String> kafkaProducer, String topic) {
    this.kafkaProducer = kafkaProducer;
    this.topic = topic;
  }

  void publish() throws Exception {
    var record = new ProducerRecord<>(topic, "A", "A");
    kafkaProducer.send(record, (metadata, exception) -> {
      System.out.println("ACK: " + metadata);
      Tracing.currentTracer().currentSpan().tag("ack", "yes");
      //}).get(); // Synchronous send
    }); // Async send
  }
}
