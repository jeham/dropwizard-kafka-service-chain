package eu.hammarback;

import io.dropwizard.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.hibernate.validator.constraints.NotBlank;

import java.util.Properties;

public class OrderProcessingConfiguration extends Configuration {

  public static final String VALID_ORDERS_TOPIC = "valid-orders";
  public static final String PROCESSED_ORDERS_TOPIC = "processed-orders";

  @NotBlank
  public String bootstrapServers;

  public Properties getKafkaStreamsConfig() {
    Properties streamsConfig = new Properties();
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-processing-service");
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfig.put(StreamsConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
    streamsConfig.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
    streamsConfig.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
    streamsConfig.put(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), "true");
    return streamsConfig;
  }

}
