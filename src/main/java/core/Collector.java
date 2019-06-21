package core;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.LinkedHashMap;

public class Collector {
    private static final Logger log = LoggerFactory.getLogger(CollectorMain.class);
    private Consumer consumer;
    private int localMsgCount = 0; 

    public Collector(Consumer consumer) {
        this.consumer = consumer;

        Runtime.getRuntime().addShutdownHook(new Thread() 
        { 
            public void run() 
            { 
                logStats();
            } 
        }); 
    }

    public void collect(int timeOut) throws PulsarClientException{
        while(true) {
            // Wait until a message is available
            Message<byte[]> msg = consumer.receive(timeOut, TimeUnit.MILLISECONDS);
            if (msg == null){
                continue;
            }
            // Extract the message as a printable string and then log
            String content = new String(msg.getData());
            String orderingKey = new String(msg.getOrderingKey());
            log.info("Received message: {} ID {}", content, msg.getMessageId());
            
            // Acknowledge processing of the message so that it can be deleted
            consumer.acknowledge(msg);
            this.localMsgCount++;
        }
    }

    public void collect(int timeOut, Producer producer) 
        throws PulsarClientException {

        while(true) {
            // Wait until a message is available
            Message<byte[]> msg 
                = this.consumer.receive(timeOut, TimeUnit.MILLISECONDS);
            if (msg == null){
                continue;
            }
            // Extract the message as a printable string and then log
            String payload = new String(msg.getData());
            String orderingKey = new String(msg.getOrderingKey());
            log.info("Received message: {} ID {} Ordering Key: {}", 
                payload, msg.getMessageId(), orderingKey);

            try {
                payload = generateJson(payload);

            } catch (ParseException e) {
                log.error(e.getMessage());
                System.exit(1);
            }

            this.localMsgCount++;
            stream(producer, payload);

            // Acknowledge processing of the message so that it can be deleted
            consumer.acknowledge(msg);

        }
    }

    public void logStats() {
        log.info("LOCAL: Total messages received: {}", this.localMsgCount);
        log.info("Total messages received: {}", 
            this.consumer.getStats().getTotalMsgsReceived());
        log.info("Total messages received failed: {}", 
            this.consumer.getStats().getTotalReceivedFailed());
        log.info("Total acknowledgments sent: {}", 
            this.consumer.getStats().getTotalAcksSent());
        log.info("Total acknowledgments failed: {}", 
            this.consumer.getStats().getTotalAcksFailed());
    }

    private String generateJson(String jsonMessage) throws ParseException{
        JSONParser parser = new JSONParser();
        JSONObject jo = (JSONObject) parser.parse(jsonMessage); 

        Map m = new LinkedHashMap(2);
        m.put("consumerName", this.consumer.getConsumerName());
        m.put("messageCount", this.localMsgCount);

        jo.put("consumer", m);

        return jo.toJSONString();
    } 

    private void stream(Producer producer, String payload) {
        try {
            MessageId msgId = producer.newMessage()
                .value(payload.getBytes())
                .send();
            log.info("\nPublished message: {} ID: {}\n", payload, msgId);

        } catch (PulsarClientException e) {
            log.error(e.getMessage());
            log.error("Message failed to send");
        } 
    }
}