package kafka.streams.test;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class StreamProducer {
    //create logger
    Logger logger = LoggerFactory.getLogger(StreamProducer.class.getName());

    //creating local variables for secrets and keys
    String consumerKey = "HxXvn6P3xJKlbD06tagIV92DC";
    String consumerSecret = "jG10OBkQ928nQ8yv18psab7FcYpBpodMF2Hlo5ULmUlKnl9NKp";
    String token = "1346155258536464385-jr8xvgAj61mkgRDI5TBVL0NYfBb3hb";
    String secret = "Y3kiVqIn055v2vd8z9z73EMcE5Z6rnbUj0dgG5gnPnkgK";

    //List of words that are to be tracked from Twitter
    List<String> terms = Lists.newArrayList("kafka", "python", "java");

    public StreamProducer(){}

    public static void main(String[] args) {
        new StreamProducer().run();
    }

    public void run(){

        //Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        //creating Twitter client
        Client client  = createClient(msgQueue);
        client.connect();

        //create a kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg  != null){
                logger.info(msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_input", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            logger.error("Error!!! ", e);
                        }
                    }
                });
            }
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {

        logger.info("Creating Kafka producer");
        String bootstrapServer = "35.239.201.205:9092";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "3");

        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(100* 1024)); //100KB batch size

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        return kafkaProducer;
    }

    public Client createClient(BlockingQueue<String> msgQueue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(
                consumerKey,
                consumerSecret,
                token,
                secret);


        //initializing a client builder that will build the Twitter client later
        ClientBuilder clientBuilder = new ClientBuilder()
                .name("Twitter-Streaming-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        // building the Twitter client
        Client client = clientBuilder.build();

        return client;
    }
}
