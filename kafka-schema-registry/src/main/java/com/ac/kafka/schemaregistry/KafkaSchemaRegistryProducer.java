package com.ac.kafka.schemaregistry;


import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaSchemaRegistryProducer {

        public static void main(String[] args) {
            Properties properties = new Properties();
            // normal producer
            properties.setProperty("bootstrap.servers", "35.222.207.78:9092");
            properties.setProperty("acks", "all");
            properties.setProperty("retries", "10");
            // avro part
            properties.setProperty("key.serializer", StringSerializer.class.getName());
            properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
            properties.setProperty("schema.registry.url", "http://35.222.207.78:8081");

            Producer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);

            String topic = "customer-avro";

            // copied from avro examples
            Customer customer = Customer.newBuilder()
                    .setAge(38)
                    .setFirstName("Saikat")
                    .setLastName("Sarker")
                    .setHeight(163f)
                    .setWeight(65f)
                    .setPhoneNumber("123")
                    .setEmail("new@old.com")
                    .build();

            ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(
                    topic, customer
            );

            System.out.println(customer);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata);
                    } else {
                        exception.printStackTrace();
                    }
                }
            });

            producer.flush();
            producer.close();

        }
}
