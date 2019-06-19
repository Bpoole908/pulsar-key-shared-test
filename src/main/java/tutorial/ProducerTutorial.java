package tutorial;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.IntStream;
import java.util.List;
import java.util.Map;
/**
 *  Run producer: mvn exec:java -Dexec.mainClass=tutorial.ProducerTutorial
 */
public class ProducerTutorial {
    private static final Logger log = LoggerFactory.getLogger(ProducerTutorial.class);
    private static final int  HASH_RANGE_SIZE = 2 << 15;

    // Extract environmental variables
    private static final String SERVICE_URL = System.getenv("SERVICE_URL");
    private static final String SERVICE_HTTP_URL = System.getenv("SERVICE_HTTP_URL");
    private static final String TOPIC =  System.getenv("TOPIC");
    private static final int N_MESSAGES = Integer.parseInt(System.getenv("N_MESSAGES"));
    private static final String SUBSCRIPTION = System.getenv("SUBSCRIPTION");
    private static final int BEFORE_START = Integer.parseInt(System.getenv("BEFORE_START"));
    public static void main(String[] args) throws IOException {
        
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(SERVICE_URL)
            .build();
            
        // Sleep consumer while cluster initializes.
        sleep(BEFORE_START);

        try{
            log.info("Sending {} messages", N_MESSAGES);
            PseudoStream pseudoStream = new PseudoStream(SERVICE_HTTP_URL, HASH_RANGE_SIZE);
            Produces produce = new Produces(client,TOPIC, N_MESSAGES);
            produce.stream(1, pseudoStream, TOPIC, SUBSCRIPTION);
        } catch (PulsarClientException | PulsarAdminException | ConsumerAssignException e){
            log.error(e.getMessage());
            System.exit(1);
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