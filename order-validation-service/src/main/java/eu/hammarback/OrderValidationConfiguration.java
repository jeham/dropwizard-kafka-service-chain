package eu.hammarback;

import io.dropwizard.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hibernate.validator.constraints.NotBlank;

import java.util.Properties;

public class OrderValidationConfiguration extends Configuration {

  public static final String PLACED_ORDERS_TOPIC = "placed-orders";
  public static final String VALID_ORDERS_TOPIC = "valid-orders";
  public static final String INVALID_ORDERS_TOPIC = "invalid-orders";

  @NotBlank
  public String bootstrapServers;

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

  public Properties getKafkaConsumerConfig() {
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "order-validation");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return consumerConfig;
  }

}
