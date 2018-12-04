package eu.hammarback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.AutoCloseableManager;
import io.dropwizard.setup.Environment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static eu.hammarback.OrderPlacementConfiguration.INVALID_ORDERS_TOPIC;
import static eu.hammarback.OrderPlacementConfiguration.PROCESSED_ORDERS_TOPIC;
import static java.lang.String.format;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.ok;
import static javax.ws.rs.core.Response.status;

public class OrderPlacementApplication extends Application<OrderPlacementConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Map<String, AsyncResponse> pendingOrderCreations = new ConcurrentHashMap<>();

  private ObjectMapper objectMapper;

  @Override
  public void run(final OrderPlacementConfiguration config, final Environment environment) {
    this.objectMapper = environment.getObjectMapper()
        .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(INDENT_OUTPUT, true)
        .setSerializationInclusion(NON_NULL);

    StreamsBuilder builder = new StreamsBuilder();

    builder.<String, String>stream(INVALID_ORDERS_TOPIC).foreach((key, value) -> {
      String message = format("Order with ID [%s] is invalid, correlationId: %s", value, key);
      logger.info(message);
      resume(key, status(BAD_REQUEST).entity(ImmutableMap.of("message", message)));
    });

    builder.<String, String>stream(PROCESSED_ORDERS_TOPIC).foreach((key, value) -> {
      Order order = fromJson(value, Order.class);
      logger.info("Successfully created order with ID [{}], correlationId: {}", order.orderId, key);
      resume(key, ok(order));
    });

    KafkaStreams streams = new KafkaStreams(builder.build(), config.getKafkaStreamsConfig());
    streams.start();

    Producer<String, String> messageProducer = new KafkaProducer<>(config.getKafkaProducerConfig());

    environment.lifecycle().manage(new AutoCloseableManager(messageProducer));
    environment.lifecycle().manage(new AutoCloseableManager(streams::close));

    environment.jersey().register(new OrderPlacementResource(messageProducer, pendingOrderCreations, objectMapper));
  }

  private void resume(String key, Response.ResponseBuilder builder) {
    Optional.ofNullable(pendingOrderCreations.remove(key)).ifPresent(response -> {
      response.resume(builder.build());
    });
  }

  public <T> T fromJson(String string, Class<T> clazz) {
    try {
      return objectMapper.readValue(string, clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(final String[] args) throws Exception {
    new OrderPlacementApplication().run(args);
  }

}
