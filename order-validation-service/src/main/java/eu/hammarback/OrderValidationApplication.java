package eu.hammarback;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.AutoCloseableManager;
import io.dropwizard.setup.Environment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static eu.hammarback.OrderValidationConfiguration.*;
import static java.util.Collections.singleton;

public class OrderValidationApplication extends Application<OrderValidationConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public void run(final OrderValidationConfiguration config, final Environment environment) {
    ObjectMapper objectMapper = environment.getObjectMapper()
        .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(INDENT_OUTPUT, true)
        .setSerializationInclusion(NON_NULL);

    Producer<String, String> messageProducer = new KafkaProducer<>(config.getKafkaProducerConfig());
    Consumer<String, String> messageConsumer = new KafkaConsumer<>(config.getKafkaConsumerConfig());

    environment.lifecycle().manage(new AutoCloseableManager(messageProducer));
    environment.lifecycle().manage(new AutoCloseableManager(messageConsumer));

    environment.jersey().disable();

    new Thread(() -> {
      messageConsumer.subscribe(singleton(PLACED_ORDERS_TOPIC));
      while (true) {
        for (ConsumerRecord<String, String> event : messageConsumer.poll(Duration.ofSeconds(10))) {
          try {
            String correlationId = event.key();
            String json = event.value();
            OrderPlacedEvent orderPlacedEvent = objectMapper.readValue(json, OrderPlacedEvent.class);

            final ProducerRecord<String, String> record;
            if (orderPlacedEvent.isValid()) {
              logger.info("Order with ID [{}] is VALID, correlationId: {}", orderPlacedEvent.orderId, correlationId);
              record = new ProducerRecord<>(VALID_ORDERS_TOPIC, correlationId, json);
            } else {
              logger.info("Order with ID [{}] is INVALID, correlationId: {}", orderPlacedEvent.orderId, correlationId);
              record = new ProducerRecord<>(INVALID_ORDERS_TOPIC, correlationId, orderPlacedEvent.orderId);
            }
            messageProducer.send(record);
          } catch (IOException e) {
            logger.warn("Unable to parse event: {}", e.getMessage());
          }
        }
      }
    }).start();
  }

  public static void main(final String[] args) throws Exception {
    new OrderValidationApplication().run(args);
  }

}
