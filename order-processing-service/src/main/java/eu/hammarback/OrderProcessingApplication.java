package eu.hammarback;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.AutoCloseableManager;
import io.dropwizard.setup.Environment;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static eu.hammarback.OrderProcessingConfiguration.PROCESSED_ORDERS_TOPIC;
import static eu.hammarback.OrderProcessingConfiguration.VALID_ORDERS_TOPIC;

public class OrderProcessingApplication extends Application<OrderProcessingConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public void run(final OrderProcessingConfiguration config, final Environment environment) {
    ObjectMapper objectMapper = environment.getObjectMapper()
        .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(INDENT_OUTPUT, true)
        .setSerializationInclusion(NON_NULL);

    JsonConverter jsonConverter = new JsonConverter(objectMapper);

    StreamsBuilder builder = new StreamsBuilder();
    builder.<String, String>stream(VALID_ORDERS_TOPIC)
        .map((key, value) -> {
          Order order = jsonConverter.fromJson(value, Order.class);
          logger.info("Processing order with ID [{}], correlationId: {}", order.orderId, key);
          order.process();
          String json = jsonConverter.toJson(order);
          return new KeyValue<>(key, json);
        })
        .filter((key, value) -> value != null)
        .to(PROCESSED_ORDERS_TOPIC);

    KafkaStreams streams = new KafkaStreams(builder.build(), config.getKafkaStreamsConfig());
    streams.start();

    environment.lifecycle().manage(new AutoCloseableManager(streams::close));
    environment.jersey().disable();
  }

  public static void main(final String[] args) throws Exception {
    new OrderProcessingApplication().run(args);
  }

}
