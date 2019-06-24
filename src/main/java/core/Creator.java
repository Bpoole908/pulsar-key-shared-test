package core;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;

import org.json.simple.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.IntStream;

import com.sun.corba.se.pept.transport.Selector;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.UUID;

public class Creator {
    private static final Logger log = LoggerFactory.getLogger(Creator.class);
    private static final int UNIT_TIME = 1000; // milliseconds
    private Producer producer;
    
    public Creator(Producer producer) {
        this.producer = producer;
    }
    
    public void stream(int nMessages, int messagesPerSecond,
        PseudoStream pseudoStream, String topic, String subscription) 
        throws PulsarClientException, 
               ConsumerAssignException, 
               PulsarAdminException {
      
        IntStream.range(1, nMessages+1).forEach(i -> {
            try{
                //long startTime = System.nanoTime();
                pseudoStream.managePseudoConsumers(topic, subscription);
                //long endTime = System.nanoTime();
                //log.debug("Run time: {} ms", (endTime - startTime) / 1e6);
            } catch(PulsarClientException | PulsarAdminException | ConsumerAssignException e){
                log.error(e.getMessage());
                System.exit(1);
            }
           
            String payload = String.format("fake-payload-%d", i);
            String orderingKey = UUID.randomUUID().toString();

            // Create a pseudo stream that predicts which consumers will recieve 
            // each message based on the ordering key's hashing slot.
            String consumerName = pseudoStream.stream(orderingKey.getBytes());
            int slot = pseudoStream.getSlot(orderingKey.getBytes());
            int range = pseudoStream.getHashRange(consumerName);
            List<String> connected = pseudoStream.getConnectedConsumers();

            // Build JSON message
            String jsonMessage = generateJson(payload, orderingKey, 
                consumerName, connected, slot, range);

             try {
                // Build a message object and send message.
                MessageId msgId = this.producer.newMessage()
                    .orderingKey(orderingKey.getBytes())
                    .value(jsonMessage.getBytes())
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
        this.producer.close();
    }

    public void stream(int nMessages, int messagesPerSecond) 
        throws PulsarClientException {

        log.info("Sending {} messages", nMessages);

        IntStream.range(1, nMessages).forEach(i -> {
            String payload = String.format("hello-pulsar-%d", i);
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
        this.producer.close();
    }

    private String generateJson(String payload, String orderingKey, 
        String name, List conneceted, int slot, int range){

        JSONObject jo = new JSONObject();

        Map prediction = new LinkedHashMap(3);
        prediction.put("name", name);
        prediction.put("slot", slot);
        prediction.put("range", range);

        Map producer = new LinkedHashMap(4);
        producer.put("payload", payload);
        producer.put("orderingKey", orderingKey);
        producer.put("prediction", prediction);
        producer.put("connected", conneceted);

        jo.put("producer", producer);

        return jo.toJSONString();
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
    
