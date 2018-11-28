package eu.hammarback;

public class Order {

  public String orderId;
  public String customerId;
  public long orderAmount;
  public Long processedAt;

  public void process() {
    // Make a change to the order to indicate som processing
    this.processedAt = System.currentTimeMillis();
  }

}
