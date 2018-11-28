package eu.hammarback;

import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.Min;

public class PlaceOrderRequest {

  @NotBlank
  public String orderId;

  @NotBlank
  public String customerId;

  @Min(1)
  public long orderAmount;

}
