package core;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CollectorMain {
    private static final Logger log = LoggerFactory.getLogger(CollectorMain.class);
    private static final String SERVICE_URL = System.getenv("SERVICE_URL");
    private static final String CONSUME_TOPIC = System.getenv("TOPIC");
    private static final String PRODUCE_TOPIC = System.getenv("PRODUCE_TOPIC");
    private static final String SUBSCRIPTION = System.getenv("SUBSCRIPTION");
    private static final int SLEEP_TIME 
        = Integer.parseInt(System.getenv("SLEEP_TIME"));
    private static final int TIME_OUT 
        = Integer.parseInt(System.getenv("TIME_OUT"));

    public static void main(String[] args) throws IOException {
        
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build();

        try{
            Consumer consumer = connect(client, CONSUME_TOPIC, SUBSCRIPTION, 
                SLEEP_TIME);
            log.info("CONSUMER {} CONNECTED: {}",
                consumer.getConsumerName(), CONSUME_TOPIC);
            Collector collector = new Collector(consumer); 
            
            //collector.collect(TIME_OUT);
            Producer producer = client.newProducer()
                .topic(PRODUCE_TOPIC)
                .create();
            collector.collect(TIME_OUT, producer);
        } catch(PulsarClientException e){
            log.info(e.getMessage());
            System.exit(1);
        } finally {
            client.close();
        }
        
    }
    
    private static Consumer connect(PulsarClient client, String topic, 
        String subscription, int sleepTime) {
        
        while (true) {
            try {
                return client.newConsumer()
                    .topic(CONSUME_TOPIC)
                    .subscriptionName(SUBSCRIPTION)
                    .subscriptionType( SubscriptionType.Key_Shared)
                    .subscribe();

            } catch(PulsarClientException e) {
                log.error(e.getMessage());
                sleep(sleepTime);
            }
        }
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