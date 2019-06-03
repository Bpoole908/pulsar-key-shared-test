package tutorial;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.IntStream;

public class ProducerTutorial {
    private static final Logger log = LoggerFactory.getLogger(ProducerTutorial.class);
    private static final String LOCAL_SERVICE_URL = "pulsar://localhost:6650";
    private static final String SERVICE_URL = "pulsar://pulsar:6650";
    private static final String TOPIC_NAME = "persistent://public/default/key_shared";
    private static final int CONSUMERS = 10;
    private static final int N_MESSAGES = 10;

    public static void main(String[] args) throws IOException {
        int nMessages;
        int consumers;
        if (args.length > 1){
            consumers =  Integer.parseInt(args[0]);
            nMessages = Integer.parseInt(args[1]);
        } else{
            consumers = CONSUMERS;
            nMessages = N_MESSAGES;
        }
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build();
        
        try{
            Produces produce = new Produces(client, consumers, TOPIC_NAME);
            produce.stream(nMessages);
            produce.logExpectedMessages();

        } catch (PulsarClientException e) {
            log.error(e.getMessage());
            System.exit(1);
        }
    }
}