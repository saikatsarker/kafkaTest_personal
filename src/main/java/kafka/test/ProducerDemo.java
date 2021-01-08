package kafka.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args){
        String bootstrapServer = "35.222.207.78:9112,35.222.207.78:9111,35.222.207.78:9113";
        //String bootstrapServer = "35.222.207.78:9092";

        Properties prop =  new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "100");
        prop.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        prop.setProperty("security.protocol", "PLAINTEXT");
        int partition = 3;
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("kafkatopic2", partition, "data","this is not a Hello World! project");

        producer.send(producerRecord);

        producer.flush();
        producer.close();
    }
}
