package tutorial;

import org.apache.pulsar.broker.service.HashRangeStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.lang.reflect.Field;
import java.util.HashMap;

public class PseudoStream{
    private static final Logger log = LoggerFactory.getLogger(PseudoStream.class);
    private HashRangeStickyKeyConsumerSelector selector;
    private PulsarAdmin admin;
    private int hashRangeSize;
    private Map<Consumer, Integer> consumerRange; // <mockid, hashrange>
    private Map<Consumer, String> consumers; // <mockid, consumer broker name>
    private Map<String, Integer> expectedMessages; // <consumer broker name, expected messages>

    PseudoStream(String serviceHttpUrl, int hashRangeSize) throws PulsarClientException{
        this.selector = new HashRangeStickyKeyConsumerSelector(hashRangeSize);
        this.hashRangeSize = hashRangeSize;
        this.consumers = new HashMap<>();
        this.expectedMessages = new HashMap<>();

        // Build the pulsar admin to gain permision to access broker/client info
         this.admin = PulsarAdmin.builder()
            .serviceHttpUrl(serviceHttpUrl)
            .build();
    }

    public static List<ConsumerStats> getConsumerStats(PulsarAdmin admin, 
        String topic, String subscription) throws PulsarClientException, PulsarAdminException {   
                
        Map<String, SubscriptionStats> subscriptionStats = admin.topics()
            .getStats(topic)
            .subscriptions;
        List<ConsumerStats> consumers = subscriptionStats.get(subscription)
            .consumers;

        log.info("Total Consumers: {}", consumers.size());

        return consumers;
    }

    public void selectConsumer(byte[] orderingKey) {
        Consumer targetConsumer = selector.select(orderingKey);
        String name = this.consumers.get(targetConsumer);
        int message_total = this.expectedMessages.get(name) + 1;

        this.expectedMessages.put(name, message_total);
    }

    public void logExpectedMessages(int actualMessages){
        log.info("Extracted consumerRange: {}", this.consumerRange);
        for (Map.Entry<Consumer, Integer>  consumer: this.consumerRange.entrySet()){
            log.info("Consumer: {} Range: {}", 
                this.consumers.get(consumer.getKey()), consumer.getValue());
        }
        log.info("Expected messages: {} Total: {}", 
            this.expectedMessages, actualMessages);
    }
    
    public int getSlot(byte[] orderingKey){
        return Murmur3_32Hash.getInstance()
            .makeHash(orderingKey) % this.hashRangeSize;
    }
    // TODO: rework to check efficiently check for new consumers every message.
    public void initializePseudoConsumers(String topic, String subscription) 
        throws PulsarClientException, PulsarAdminException, ConsumerAssignException {

        List<ConsumerStats> consumerStats = getConsumerStats(this.admin, topic, subscription);
        for (int i=0; i<consumerStats.size(); i++){
            try{
                Consumer mockConsumer = mock(Consumer.class);
                this.consumers.put(mockConsumer, consumerStats.get(i).consumerName);
                this.expectedMessages.put(consumerStats.get(i).consumerName, 0);
                selector.addConsumer(mockConsumer);
            } catch(ConsumerAssignException e){
                log.error(e.getMessage());
                System.exit(1);
            }
        }
        // Override security to get private variable consumerRange,
        // which contains the hash ranges for each mock consumer.
        try{
            Field field = selector.getClass().getDeclaredField("consumerRange");
            field.setAccessible(true);
            this.consumerRange = (Map) field.get(selector);
        } catch(NoSuchFieldException | IllegalAccessException e){
            log.error(e.getMessage());
            System.exit(1);
        }
    }
}