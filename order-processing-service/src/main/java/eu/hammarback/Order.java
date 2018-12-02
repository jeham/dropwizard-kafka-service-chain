package eu.hammarback;

import org.apache.commons.lang3.RandomUtils;

public class Order {

  public String orderId;
  public String customerId;
  public long orderAmount;
  public Long processedAt;

  public void process() {
    // Make a change to the order to indicate some processing
    this.processedAt = System.currentTimeMillis();

    // Make 10% of all orders slow to process
    if (RandomUtils.nextInt(0, 9) == 0) {
      try {
        Thread.sleep(3000L);
      } catch (InterruptedException e) {
        // Ignore
      }
    }

  }

}
