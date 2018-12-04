package eu.hammarback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.UUID;

import static eu.hammarback.OrderPlacementConfiguration.PLACED_ORDERS_TOPIC;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static javax.ws.rs.core.Response.status;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OrderPlacementResource {

  private static final long REQUEST_TIMEOUT_SECONDS = 3;

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Map<String, AsyncResponse> pendingOrderCreations;
  private final ObjectMapper objectMapper;
  private final Producer<String, String> messageProducer;

  public OrderPlacementResource(Producer<String, String> messageProducer,
                                Map<String, AsyncResponse> pendingOrderCreations,
                                ObjectMapper objectMapper) {

    this.messageProducer = messageProducer;
    this.pendingOrderCreations = pendingOrderCreations;
    this.objectMapper = objectMapper;
  }

  @POST
  @Path("place-order")
  public void placeOrder(@Valid @NotNull PlaceOrderRequest request, @Suspended AsyncResponse asyncResponse) {
    String correlationId = UUID.randomUUID().toString();

    asyncResponse.setTimeout(REQUEST_TIMEOUT_SECONDS, SECONDS);
    asyncResponse.setTimeoutHandler(response -> {
      logger.info("Removing pending order response due to timeout, correlationId: {}", correlationId);
      pendingOrderCreations.remove(correlationId);
      response.resume(createErrorResponse(request.orderId));
    });

    ProducerRecord<String, String> record = new ProducerRecord<>(PLACED_ORDERS_TOPIC, correlationId, toJson(request));
    messageProducer.send(record, (metadata, exception) -> {
      if (exception == null) {
        logger.info("Awaiting processing of order with ID [{}], correlationId: {}", request.orderId, correlationId);
        pendingOrderCreations.put(correlationId, asyncResponse);
      } else {
        logger.warn("Unable to create order with ID [{}]: {}", request.orderId, exception.getMessage());
        asyncResponse.resume(status(INTERNAL_SERVER_ERROR).build());
      }
    });
  }

  private String toJson(Object object) {
    try {
      return objectMapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private Response createErrorResponse(String orderId) {
    return status(SERVICE_UNAVAILABLE).entity(ImmutableMap.of(
        "message", format("Operation timed out while waiting for processing of order with ID [%s]", orderId))
    ).build();
  }

}
