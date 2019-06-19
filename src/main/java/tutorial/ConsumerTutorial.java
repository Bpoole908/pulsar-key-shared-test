package tutorial;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class ConsumerTutorial {
    private static final Logger log = LoggerFactory.getLogger(ConsumerTutorial.class);
    private static final String SERVICE_URL = System.getenv("SERVICE_URL");
    private static final String TOPIC = System.getenv("TOPIC");
    private static final String SUBSCRIPTION = System.getenv("SUBSCRIPTION");
    private static final int BEFORE_START = Integer.parseInt(System.getenv("BEFORE_START"));
    private static final int TIME_OUT = Integer.parseInt(System.getenv("TIME_OUT"));
    private static Consumer<byte[]> consumer;
    private static int localCount = 0;
    public static void main(String[] args) throws IOException {
        
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build();

        // Sleep consumer while cluster initializes.
        sleep(BEFORE_START);

        try{
            consumer = client.newConsumer()
                .topic(TOPIC)
                .subscriptionName(SUBSCRIPTION)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();
        } catch(PulsarClientException e){
            e.printStackTrace();
            log.info(e.getMessage());
            System.exit(1);
        }
      
        Runtime.getRuntime().addShutdownHook(new Thread() 
        { 
          public void run() 
          { 
            logStats(consumer);
          } 
        }); 

        log.info("CONSUMER {} CONNECTED: {}", consumer.getConsumerName(), TOPIC);
        while(true) {
            // Wait until a message is available
            Message<byte[]> msg = consumer.receive(TIME_OUT, TimeUnit.MILLISECONDS);
            if (msg == null){
                //logStats(consumer);
                continue;
            }
            // Extract the message as a printable string and then log
            String content = new String(msg.getData());
            String orderingKey = new String(msg.getOrderingKey());
            log.info("Received message '{}' with ID {}", content, msg.getMessageId());
            log.info("Ordering key: {}", orderingKey );
            localCount++;
            // Acknowledge processing of the message so that it can be deleted
            consumer.acknowledge(msg);
        }
    }

    public static void logStats(Consumer consumer){
        log.info("LOCAL: Total messages received: {}", localCount);
        log.info("Total messages received: {}", 
            consumer.getStats().getTotalMsgsReceived());
        log.info("Total messages received failed: {}", 
            consumer.getStats().getTotalReceivedFailed());
        log.info("Total acknowledgments sent: {}", 
            consumer.getStats().getTotalAcksSent());
        log.info("Total acknowledgments failed: {}", 
            consumer.getStats().getTotalAcksFailed());
    }

    private static void sleep(int ms){
        try {
            Thread.sleep(ms);
        }
        catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }
}