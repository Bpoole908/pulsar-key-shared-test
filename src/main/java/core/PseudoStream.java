package core;

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
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

public class PseudoStream {
    private static final Logger log = LoggerFactory.getLogger(PseudoStream.class);
    private HashRangeStickyKeyConsumerSelector selector;
    private PulsarAdmin admin;
    private int hashRangeSize;
    // Tracks currently connected consumers hash ranges
    private Map<Consumer, Integer> consumerRange 
        = new LinkedHashMap<Consumer, Integer>();
    // Tracks history of all consumers by consumer object ID (generated via mock)
    private Map<Consumer, String> consumers 
        = new LinkedHashMap<Consumer, String>(); 
    // Tracks history of all consumers that were connected by consumer name
    private Map<String, Consumer> consumerNames 
        = new LinkedHashMap<String, Consumer>();
    // Tracks previously connected consumers from 1 message ago.
    private Map<String, Consumer> prevConnectedConsumers 
        = new LinkedHashMap<String, Consumer>(); 
     // Tracks history of messages sent to all consumers that were once connected.
    private Map<String, Integer> msgDist 
        = new LinkedHashMap<String, Integer>();

    PseudoStream(String serviceHttpUrl, int hashRangeSize) 
        throws PulsarClientException {

        this.selector = new HashRangeStickyKeyConsumerSelector(hashRangeSize);
        this.hashRangeSize = hashRangeSize;
        // Create admin to access broker information (i.e. consumer stats). 
         this.admin = PulsarAdmin.builder()
            .serviceHttpUrl(serviceHttpUrl)
            .build();
    }

    public static List<ConsumerStats> getConsumerStats(PulsarAdmin admin, 
        String topic, String subscription) 
        throws PulsarClientException, 
               PulsarAdminException {   
                
        Map<String, SubscriptionStats> subscriptionStats = admin.topics()
            .getStats(topic)
            .subscriptions;
        List<ConsumerStats> consumerStats = subscriptionStats.get(subscription)
            .consumers;

        log.info("Total consumers: {}", consumerStats.size());
        
        return consumerStats;
    }

    public String stream(byte[] orderingKey) {
        // Selects which consumer should receive the message based on which slot
        // the ordering key "fits."
        Consumer targetConsumer = selector.select(orderingKey);
        String name = this.consumers.get(targetConsumer);
        // Iterate the message distribution.
        this.msgDist.put(name, this.msgDist.get(name)+1);

        return name;
    }

    public void managePseudoConsumers(String topic, String subscription) 
        throws PulsarClientException, 
               PulsarAdminException, 
               ConsumerAssignException {
        // Extract stats on currently connect consumers
        List<ConsumerStats> consumerStats 
            = getConsumerStats(this.admin, topic, subscription);
        Map<String, Consumer> connectedConsumers 
            = new LinkedHashMap<String, Consumer>();

        // Get consumer name from consumer stats and create a Map of 
        // currently conneceted consumers. This is used for set mathematics to
        // determin new and dropped consumers.
        for (ConsumerStats c : consumerStats) {
            String consumerName = c.consumerName;
            connectedConsumers.put(consumerName, consumerNames.get(consumerName));
        }
        
        // Calculate the difference and similarties between current
        // and previously connected consumers. 
        Set<String> newConsumers 
            = new LinkedHashSet<String>(connectedConsumers.keySet());
        Set<String> droppedConsumers
             = new LinkedHashSet<String>(this.prevConnectedConsumers.keySet());
        newConsumers.removeAll(this.prevConnectedConsumers.keySet());
        droppedConsumers.removeAll(connectedConsumers.keySet());
        log.debug("Left space: {}", newConsumers);
        log.debug("Right space: {}", droppedConsumers);

        // Drop consumers from hash range if they are no longer connected.
        for (String droppedConsumerName : droppedConsumers) {
            Consumer consumer = this.consumerNames.get(droppedConsumerName);
            selector.removeConsumer(consumer);
        }
        // Once consumers have been dropped then add any new consumers to hash
        // range.
        for (String newConsumerName: newConsumers) {
            try {
                Consumer consumer = mock(Consumer.class);
                this.consumers.put(consumer, newConsumerName);
                this.consumerNames.put(newConsumerName, consumer);
                this.msgDist.put(newConsumerName, 0);
                // Add to Pulsar's hash range tracker.
                selector.addConsumer(consumer);
            } catch(ConsumerAssignException e) {
                log.error(e.getMessage());
                System.exit(1);
            }
        }

        // Extract each consumer's hashing range.
        getConsumerRange();
       
        // When a consumer is dorpped or added log the hash ranges.
        if (!droppedConsumers.isEmpty() || !newConsumers.isEmpty()) {
            logHashRanges();
        }

        // Store current consumers for the next iteration.
        this.prevConnectedConsumers.clear();
        this.prevConnectedConsumers.putAll(connectedConsumers);
    }

    public int getSlot(byte[] orderingKey){
        return Murmur3_32Hash.getInstance()
            .makeHash(orderingKey) % this.hashRangeSize;
    }

    public int getHashRange(String consumerName){
        Consumer consumer = this.consumerNames.get(consumerName);
        return this.consumerRange.get(consumer);
    }

    public int getConsumerMsgCount(String name){
        return this.msgDist.get(name);
    }

    public Map getConnectedConsumersRanges(){
        Map<String, Integer> connectedConsumerRanges = new LinkedHashMap<>();

        for (Map.Entry<Consumer, Integer>  consumer: this.consumerRange.entrySet()) {
            String key =this.consumers.get(consumer.getKey());
            int value = consumer.getValue();
            connectedConsumerRanges.put(key, value);
        }

        return connectedConsumerRanges;
    }

    public List getConnectedConsumers() {
        List<String> connectedConsumers = new ArrayList<String>();
        connectedConsumers.addAll(this.prevConnectedConsumers.keySet());
    
        return connectedConsumers;
    }

    public void logMsgDistribution(){
        log.info("Message distribution:, {}", this.msgDist);
    }

    public void logMsgDistribution(String name, int slot){
        log.info("Name: {} Slot: {} Message distribution: {}",
            name, slot, this.msgDist);
    }

    public void logHashRanges(){
        log.info("NEW HASHS - {}", getConnectedConsumersRanges());
    }
 
    /**
     * Override security to get private variable consumerRange,
     * which contains the hash ranges for each mock consumer.
     */
    private void getConsumerRange() {
        try{
            Field field = selector.getClass().getDeclaredField("consumerRange");
            field.setAccessible(true);
            this.consumerRange = (Map) field.get(selector);
        } catch(NoSuchFieldException | IllegalAccessException e){
            log.error("Failed to access consumerRange:", e.getMessage());
            System.exit(1);
        }
    }
}