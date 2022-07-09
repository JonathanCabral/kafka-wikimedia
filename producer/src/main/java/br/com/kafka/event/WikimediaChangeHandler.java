package br.com.kafka.event;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    private final String topic;
    private final KafkaProducer<String, String> producer;

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {}

    @Override
    public void onClosed() {
        //Close also does the flush
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        log.info("Sending message to the topic: " + topic + " Message: " + messageEvent.getData() );
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) {}

    @Override
    public void onError(Throwable t) {
        log.error("Error while reading from stream", t);
    }
}
