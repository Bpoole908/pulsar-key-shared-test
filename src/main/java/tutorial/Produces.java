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

import java.util.stream.IntStream;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.mockito.Mockito.mock;

public class Produces {
    private static final Logger log = LoggerFactory.getLogger(Produces.class);
    private int nConsumers;
    private HashRangeStickyKeyConsumerSelector selector = new HashRangeStickyKeyConsumerSelector();
    private PulsarClient client;
    private Consumer[] consumers;
    private int[] expectedMessages;
    private Producer<byte[]> producer;

    public Produces(PulsarClient client, int nConsumers, String topicName) throws PulsarClientException {
        this.nConsumers = nConsumers;
        this.client = client;
        this.consumers = new Consumer[nConsumers];
        this.expectedMessages = new int[nConsumers];
        this.producer = client.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .create();
        try {
            initializPseudoConsumers();
        } catch (ConsumerAssignException e){
                log.error(e.getMessage());
                System.exit(1);
        }
    }

    public void stream(int nMessages) throws PulsarClientException {
        log.info("Sending {} messages to {} consumers", nMessages, this.nConsumers);

        IntStream.range(1, nMessages+1).forEach(i -> {
            String payload = String.format("hello-pulsar-%d", i);
            String orderingKey = UUID.randomUUID().toString();
            pseudoStream(this.selector.select(orderingKey.getBytes()));
             try {
                // Build a message object
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
            sleep(1000);
        });

        client.close();
    }

    public void logExpectedMessages(){
        int total = IntStream.of(this.expectedMessages).sum();
        log.info("Expected messages: {} Total: {}", this.expectedMessages, total);
    }

    private void initializPseudoConsumers() throws ConsumerAssignException {
        for (int i=0; i<this.nConsumers; i++){
            Consumer mockConsumer = mock(Consumer.class);
            this.consumers[i] = mockConsumer;
            this.selector.addConsumer(mockConsumer);
        }
    }

    private void pseudoStream(Consumer selectedConsumer){
        for (int c=0; c < this.nConsumers; c++){
            if (this.consumers[c] == selectedConsumer){
                this.expectedMessages[c]++;
                break;
            }
        }
    }

    private void sleep(int ms){
        try {
            Thread.sleep(ms);
        }
        catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }

}
    
