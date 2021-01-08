package kafka.test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallbackDemo {
    public static void main(String[] args){
        //logging the process of sending data
        Logger logger = LoggerFactory.getLogger(ProducerCallbackDemo.class);
        //kafka broker ip:port
        String bootstrapServer = "35.222.207.78:9112,35.222.207.78:9111,35.222.207.78:9113";

        //setting kafka producer properties
        Properties prop =  new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "100");
        prop.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        prop.setProperty("security.protocol", "PLAINTEXT");

        //calling kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        for(int i=0; i<= 100; i++) {
            //creating a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>("kafkatopic2", "Hello World! this is "+ i);

            //asynchronously sending the produced data to the kafka topic
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //record sent successful
                        logger.info("Message metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error occurred", e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
