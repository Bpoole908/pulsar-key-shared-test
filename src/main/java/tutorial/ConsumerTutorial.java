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
    private static final String LOCAL_SERVICE_URL = "pulsar://localhost:6650";
    private static final String SERVICE_URL = "pulsar://pulsar:6650";
    private static final String TOPIC_NAME = "persistent://public/default/key_shared";
    private static final String SUBSCRIPTION_NAME = "key_shared";
    private static Consumer<byte[]> consumer;
    
    public static void main(String[] args) throws IOException {
       PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build();
                
        try{
            consumer = client.newConsumer()
                .topic(TOPIC_NAME)
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();
        } catch(PulsarClientException e){
            log.info(e.getMessage());
            System.exit(1);
        }
      
        log.info("CONNECTED: {}", TOPIC_NAME);

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
}