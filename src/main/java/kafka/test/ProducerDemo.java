package kafka.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args){
        String bootstrapServer = "35.239.201.205:9112";

        Properties prop =  new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty("security.protocol", "PLAINTEXT");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("kafkatopic2", "this is not a Hello World! project" );

        producer.send(producerRecord);

        producer.flush();
        producer.close();
    }
}
