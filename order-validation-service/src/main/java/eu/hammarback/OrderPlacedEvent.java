package eu.hammarback;

import org.apache.commons.lang3.RandomUtils;

public class OrderPlacedEvent {

  public String orderId;
  public String customerId;
  public long orderAmount;

  public boolean isValid() {
    // Make 10% of all orders invalid
    return RandomUtils.nextInt(0, 9) != 0;
  }

}
