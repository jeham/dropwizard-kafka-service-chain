package eu.hammarback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.AutoCloseableManager;
import io.dropwizard.setup.Environment;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static eu.hammarback.OrderValidationConfiguration.*;

public class OrderValidationApplication extends Application<OrderValidationConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private ObjectMapper objectMapper;

  @Override
  public void run(final OrderValidationConfiguration config, final Environment environment) {
    this.objectMapper = environment.getObjectMapper()
        .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(INDENT_OUTPUT, true)
        .setSerializationInclusion(NON_NULL);

    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, OrderPlacedEvent>[] branchedOrders = builder.<String, String>stream(PLACED_ORDERS_TOPIC)
        .mapValues(value -> fromJson(OrderPlacedEvent.class, value))
        .branch(
            (key, event) -> {
              // Include only valid orders in first stream
              boolean valid = event.isValid();
              logger.info("Order with ID [{}] is {}, correlationId: {}", event.orderId, valid ? "VALID" : "INVALID", key);
              return valid;
            },
            (key, value) -> {
              // Include all other (i.e. invalid orders) in second stream
              return true;
            });

    // Pass the valid order on to the next topic
    branchedOrders[0].mapValues(this::toJson).to(VALID_ORDERS_TOPIC);

    // Only pass orderId as value if order is invalid
    branchedOrders[1].mapValues(value -> value.orderId).to(INVALID_ORDERS_TOPIC);

    KafkaStreams streams = new KafkaStreams(builder.build(), config.getKafkaStreamsConfig());
    streams.start();

    environment.lifecycle().manage(new AutoCloseableManager(streams::close));

    environment.jersey().disable();
  }

  private String toJson(Object object) {
    try {
      return objectMapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T fromJson(Class<T> clazz, String json) {
    try {
      return objectMapper.readValue(json, clazz);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static void main(final String[] args) throws Exception {
    new OrderValidationApplication().run(args);
  }

}
