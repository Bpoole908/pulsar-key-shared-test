public class PseudoStream{

    private HashRangeStickyKeyConsumerSelector selector;
    private Map<Consumer, Integer> consumerRange; // <mockid, hashrange>
    private Map<Consumer, String> consumers; // <mockid, consumer broker name>
    private Map<String, Integer> expectedMessages; // <consumer broker name, expected messages>

    PseudoStream(String serviceHttpUrl, int hashRangeSize){
        this.hashRangeSize = hashRangeSize;
        this.selector = new HashRangeStickyKeyConsumerSelector(hashRangeSize);
        this.consumers = new HashMap<>();
        this.expectedMessages = new HashMap<>();

        PulsarAdmin admin = PulsarAdmin.builder()
            .serviceHttpUrl(serviceHttpUrl)
            .build();
    }

    public List<ConsumerStats> getConsumerStats(PulsarAdmin admin) 
        throws PulsarClientException, PulsarAdminException {   
                
        Map<String, SubscriptionStats> subscriptionStats = admin.topics()
            .getStats(TOPIC)
            .subscriptions;
        List<ConsumerStats> consumers = subscriptionStats.get(SUBSCRIPTION)
            .consumers;

        log.info("Total Consumers: {}", consumers.size());

        return consumers;
    }
    
    private void initializPseudoConsumers(List<ConsumerStats> consumerStats) {
        log.info("{}", consumerStats);
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
        // Overide security to get private variable consumerRange,
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

    private void pseudoStream(Consumer selectedConsumer) {
        String name = this.consumers.get(selectedConsumer);
        int message_total = this.expectedMessages.get(name) + 1;
        this.expectedMessages.put(name, message_total);
    }

}