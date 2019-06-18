package tutorial;

import java.io.IOException;
import java.util.Arrays;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerTutorial {
    private static final Logger log = LoggerFactory.getLogger(ConsumerTutorial.class);
    private static final String SERVICE_URL = System.getenv("SERVICE_URL");
    private static final String TOPIC = System.getenv("TOPIC");
    private static final String SUBSCRIPTION = System.getenv("SUBSCRIPTION");
    private static final int BEFORE_START = Integer.parseInt(System.getenv("BEFORE_START"));
    
    private static Consumer<byte[]> consumer;
    
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
      
        log.info("CONSUMER {} CONNECTED: {}", consumer.getConsumerName(), TOPIC);

        do {
            // Wait until a message is available
            Message<byte[]> msg = consumer.receive();

            // Extract the message as a printable string and then log
            String content = new String(msg.getData());
            log.info("Received message '{}' with ID {}", content, msg.getMessageId());
            log.info("Ordering key: {}", msg.getOrderingKey());
            
            // Acknowledge processing of the message so that it can be deleted
            consumer.acknowledge(msg);
        } while (true);
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