package tutorial;

import org.apache.pulsar.broker.service.HashRangeStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.apache.pulsar.common.policies.data.ConsumerStats;
import java.util.List;
import java.util.Map;
import java.lang.reflect.Field;
import java.util.HashMap;

import java.util.stream.IntStream;

import com.sun.corba.se.pept.transport.Selector;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.mockito.Mockito.mock;

public class Produces {
    private static final Logger log = LoggerFactory.getLogger(Produces.class);
    private static final int UNIT_TIME = 1000; // miliseconds
    private int nMessages;
    private PulsarClient client;
    private Producer<byte[]> producer;
   
    
    public Produces(PulsarClient client, String topicName, int nMessage) 
        throws PulsarClientException {

        this.client = client;
        this.nMessages = nMessages;
        this.producer = client.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .create();
    }
    
    public void stream(int messagesPerSecond, PseudoStream pseudoStream) 
        throws PulsarClientException {

        log.info("Sending {} messages to {} consumers", this.nMessages, this.nConsumers);

        IntStream.range(1, this.nMessages+1).forEach(i -> {
            String payload = String.format("hello-pulsar-%d", i);
            // Randomly generate ordering key
            String orderingKey = UUID.randomUUID().toString();

            // Create a pseudo stream that predicts which consumers will recieve 
            // each message based on the odering key (which generates a 
            // hashing slot).
            int slot = Murmur3_32Hash.getInstance()
                .makeHash(orderingKey.getBytes()) % this.hashRangeSize;
            pseudoStream(selector.select(orderingKey.getBytes()));

             try {
                // Build a message object and send message.
                MessageId msgId = this.producer.newMessage()
                    .orderingKey(orderingKey.getBytes())
                    .value(payload.getBytes())
                    .send();

                log.info("Published message: '{}' ID: {} Slot: {}", payload, msgId, slot);
                //log.info("Ordering key UUID: {} Bytes: {}", orderingKey, orderingKey.getBytes());
            } catch (PulsarClientException e) {
                log.error(e.getMessage());
                System.exit(1);
            }
            sleep(UNIT_TIME / messagesPerSecond);
        });
        client.close();
    }

    public void stream(int messagesPerSecon) 
        throws PulsarClientException {

        log.info("Sending {} messages", this.nMessages);

        IntStream.range(1, this.nMessages+1).forEach(i -> {
            String payload = String.format("hello-pulsar-%d", i);
            // Randomly generate ordering key
            String orderingKey = UUID.randomUUID().toString();

            try {
                // Build a message object and send message.
                MessageId msgId = this.producer.newMessage()
                    .orderingKey(orderingKey.getBytes())
                    .value(payload.getBytes())
                    .send();

                log.info("Published message: '{}' ID: {}", payload, msgId);
                log.info("Ordering key UUID: {} Bytes: {}", orderingKey, orderingKey.getBytes());
            } catch (PulsarClientException e) {
                log.error(e.getMessage());
                System.exit(1);
            }
            sleep(UNIT_TIME / messagesPerSecond);
        });
        client.close();
}

    public void logExpectedMessages(){
        log.info("Extracted consumerRange: {}", this.consumerRange);
        for (Map.Entry<Consumer, Integer>  consumer: this.consumerRange.entrySet()){
            log.info("Consumer: {} Range: {}", 
                this.consumers.get(consumer.getKey()), consumer.getValue());
        }
        log.info("Expected messages: {} Total: {}", 
            this.expectedMessages, this.nMessages);
    }

    private void sleep(int ms){
        try {
            Thread.sleep(ms);
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
    
