package eu.hammarback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.AutoCloseableManager;
import io.dropwizard.setup.Environment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import java.time.Duration;
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

public class OrderPlacementApplication extends Application<OrderPlacementConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public void run(final OrderPlacementConfiguration config, final Environment environment) {
    ObjectMapper objectMapper = environment.getObjectMapper()
        .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(INDENT_OUTPUT, true)
        .setSerializationInclusion(NON_NULL);

    JsonConverter jsonConverter = new JsonConverter(objectMapper);

    Producer<String, String> messageProducer = new KafkaProducer<>(config.getKafkaProducerConfig());
    Consumer<String, String> messageConsumer = new KafkaConsumer<>(config.getKafkaConsumerConfig());

    environment.lifecycle().manage(new AutoCloseableManager(messageProducer));
    environment.lifecycle().manage(new AutoCloseableManager(messageConsumer));

    Map<String, AsyncResponse> pendingOrderCreations = new ConcurrentHashMap<>();

    startMessageSubscription(messageConsumer, jsonConverter, pendingOrderCreations);

    environment.jersey().register(new OrderPlacementResource(messageProducer, pendingOrderCreations, jsonConverter));
  }

  private void startMessageSubscription(Consumer<String, String> messageConsumer,
                                        JsonConverter jsonConverter,
                                        Map<String, AsyncResponse> pendingOrderCreations) {
    new Thread(() -> {
      messageConsumer.subscribe(ImmutableList.of(INVALID_ORDERS_TOPIC, PROCESSED_ORDERS_TOPIC));
      while (true) {
        for (ConsumerRecord<String, String> event : messageConsumer.poll(Duration.ofSeconds(10))) {
          String correlationId = event.key();

          final Response.ResponseBuilder responseBuilder;
          if (INVALID_ORDERS_TOPIC.equals(event.topic())) {
            String message = format("Order with ID [%s] is invalid, correlationId: %s", event.value(), correlationId);
            logger.info(message);
            responseBuilder = Response.status(BAD_REQUEST).entity(ImmutableMap.of("message", message));
          } else if (PROCESSED_ORDERS_TOPIC.equals(event.topic())) {
            Order order = jsonConverter.fromJson(event.value(), Order.class);
            logger.info("Successfully created order with ID [{}], correlationId: {}", order.orderId, correlationId);
            responseBuilder = Response.ok(order);
          } else {
            logger.warn("Unhandled event: ", event);
            responseBuilder = Response.serverError();
          }

          Optional.ofNullable(pendingOrderCreations.remove(correlationId)).ifPresent(response -> {
            response.resume(responseBuilder.build());
          });
        }
      }
    }).start();
  }

  public static void main(final String[] args) throws Exception {
    new OrderPlacementApplication().run(args);
  }

}
