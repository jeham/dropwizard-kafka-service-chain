package eu.hammarback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.AutoCloseableManager;
import io.dropwizard.setup.Environment;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static eu.hammarback.OrderProcessingConfiguration.PROCESSED_ORDERS_TOPIC;
import static eu.hammarback.OrderProcessingConfiguration.VALID_ORDERS_TOPIC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class OrderProcessingApplication extends Application<OrderProcessingConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private ObjectMapper objectMapper;

  @Override
  public void run(final OrderProcessingConfiguration config, final Environment environment) {
    this.objectMapper = environment.getObjectMapper()
        .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(INDENT_OUTPUT, true)
        .setSerializationInclusion(NON_NULL);

    StreamsBuilder builder = new StreamsBuilder();
    builder.<String, String>stream(VALID_ORDERS_TOPIC)
        .map((key, value) -> {
          Order order = fromJson(value, Order.class);
          Stopwatch stopwatch = Stopwatch.createStarted();
          order.process();
          stopwatch.stop();
          logger.info("Order with ID [{}] processed in [{}] ms, correlationId: {}", order.orderId, stopwatch.elapsed(MILLISECONDS), key);
          return new KeyValue<>(key, toJson(order));
        }).to(PROCESSED_ORDERS_TOPIC);

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

  private <T> T fromJson(String string, Class<T> clazz) {
    try {
      return objectMapper.readValue(string, clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(final String[] args) throws Exception {
    new OrderProcessingApplication().run(args);
  }

}
