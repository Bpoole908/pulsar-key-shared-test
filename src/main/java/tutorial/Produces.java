package tutorial;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;

import java.util.stream.IntStream;

import com.sun.corba.se.pept.transport.Selector;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Produces {
    private static final Logger log = LoggerFactory.getLogger(Produces.class);
    private static final int UNIT_TIME = 1000; // milliseconds
    private int nMessages;
    private PulsarClient client;
    private Producer<byte[]> producer;
    
    public Produces(PulsarClient client, String topicName, int nMessages) 
        throws PulsarClientException {

        this.client = client;
        this.nMessages = nMessages;
        this.producer = client.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .create();
    }
    
    public void stream(int messagesPerSecond, PseudoStream pseudoStream,
        String topic, String subscription) 
        throws PulsarClientException, 
               ConsumerAssignException, 
               PulsarAdminException {
      
        IntStream.range(1, this.nMessages+1).forEach(i -> {
            try{
                //long startTime = System.nanoTime();
                pseudoStream.managePseudoConsumers(topic, subscription);
                //long endTime = System.nanoTime();
                //log.debug("Run time: {} ms", (endTime - startTime) / 1e6);
            } catch(PulsarClientException | PulsarAdminException | ConsumerAssignException e){
                log.error(e.getMessage());
                System.exit(1);
            }
           
            String payload = String.format("hello-pulsar-%d", i);
            String orderingKey = UUID.randomUUID().toString();

            // Create a pseudo stream that predicts which consumers will recieve 
            // each message based on the ordering key's hashing slot.
            int slot = pseudoStream.getSlot(orderingKey.getBytes());
            String consumerName = pseudoStream.stream(orderingKey.getBytes());

             try {
                // Build a message object and send message.
                MessageId msgId = this.producer.newMessage()
                    .orderingKey(orderingKey.getBytes())
                    .value(payload.getBytes())
                    .send();
                log.info("Published message: '{}' ID: {}", payload, msgId);
                pseudoStream.logMessageDistribution(consumerName, slot);
                   
            } catch (PulsarClientException e) {
                log.error(e.getMessage());
                log.error("Message failed to send");
            }
            sleep(UNIT_TIME / messagesPerSecond);
        });
        pseudoStream.logMessageDistribution();
        client.close();
    }

    public void stream(int messagesPerSecond) 
        throws PulsarClientException {

        log.info("Sending {} messages", this.nMessages);

        IntStream.range(1, this.nMessages).forEach(i -> {
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

    private void sleep(int ms){
        try {
            Thread.sleep(ms);
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
    
