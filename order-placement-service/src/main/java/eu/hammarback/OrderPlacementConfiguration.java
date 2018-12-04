package eu.hammarback;

import io.dropwizard.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.hibernate.validator.constraints.NotBlank;

import java.util.Properties;

public class OrderPlacementConfiguration extends Configuration {

  public static final String PLACED_ORDERS_TOPIC = "placed-orders";
  public static final String INVALID_ORDERS_TOPIC = "invalid-orders";
  public static final String PROCESSED_ORDERS_TOPIC = "processed-orders";

  @NotBlank
  public String bootstrapServers;

  public Properties getKafkaStreamsConfig() {
    Properties streamsConfig = new Properties();
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-placement");
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfig.put(StreamsConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
    return streamsConfig;
  }

  public Properties getKafkaProducerConfig() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    return producerConfig;
  }

}
