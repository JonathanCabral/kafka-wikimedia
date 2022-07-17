package br.com.kafka.producer;

import br.com.kafka.event.WikimediaChangeHandler;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangesProducer {

    private final static Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /**
         * SINCE KAFKA 3.0, THIS PROPERTIES ARE SET BY DEFAULT, AKA SAFE KAFKA PRODUCER
         * So it's not needed to be set
         */
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); Ensure that the data is properly replicated, before an ack is received
//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); Duplicates are no introduced due to network retries
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "2147483647"); //Integer.MAX_VALUE Number of times the producer will try retry due the time of timeout
//        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000"); Fail after retrying for 2 minutes
//        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");  Ensure that maximum performance while keeping messages ordering

        //properties for a high throughput producer, configuring compression, linger.ms and batch.size
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.SNAPPY.toString());
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //20ms
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        //start the producer in another thread
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}
