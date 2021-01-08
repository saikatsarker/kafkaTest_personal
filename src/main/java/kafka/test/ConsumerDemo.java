package kafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        //set the logging variable
        Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        //local variables
        String bootstrapServer = "35.222.207.78:9112,35.222.207.78:9111,35.222.207.78:9113";
        String groupId = "consumer_client_01";

        //setting consumer properties
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        //Subscribe consumer to kafka topics
        //consumer.subscribe(Pattern.compile("kafkatopic.*")); // is taking multiple topics, what are the other ways?
        consumer.subscribe(Arrays.asList("kafkatopic2"));

        //read data from the subscribed topics
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> rec : records){
                log.info("Topic:" + rec.topic() +
                        " Key:" + rec.key() +
                        " Value:" + rec.value() +
                        " Partition:" + rec.partition() +
                        " offset:" + rec.offset());
            }
        }

    }
}
