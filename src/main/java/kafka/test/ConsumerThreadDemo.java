package kafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThreadDemo {
    public static void main(String[] args) {
        new ConsumerThreadDemo().run(); // directly call the run method while constructing the class
    }
    private ConsumerThreadDemo(){

    }
    private void run(){
        //set the logging variable
        Logger log = LoggerFactory.getLogger(ConsumerThreadDemo.class.getName());

        //local variables
        String bootstrapServer = "35.222.207.78:9112,35.222.207.78:9111,35.222.207.78:9113";
        String groupId = "consumer_client_02";
        //String topics = "kafkatopic.*";
        String topics = "kafkatopic2";

        //define latch to deal multiple thread
        CountDownLatch latch = new CountDownLatch(1);
        //creating consumer runnable
        log.info("Creating consumer runnable");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch,
                bootstrapServer,
                topics,
                groupId);

        //starting thread
        log.info("Starting thread");
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        
        // adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Cought shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.error("Error occurred", e);
            }finally{
                log.info("Application closed");
            }
        }
                
        ));
        
        //latching for interruption
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Error occurred", e);
        }finally{
            log.info("Closing application");
        }

    }
    public class ConsumerRunnable implements Runnable {
        //private variables
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        //set the logging variable
        private Logger log = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch latch,
                              String bootstrapServer,
                              String topics,
                              String groupId){
            this.latch = latch;

            //setting consumer properties
            Properties prop = new Properties();
            prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            //create consumer
            consumer = new KafkaConsumer<String, String>(prop);

            //Subscribe consumer to kafka topics
            //consumer.subscribe(Pattern.compile(topics)); // is taking multiple topics, what are the other ways?
            consumer.subscribe(Arrays.asList(topics));
        }

        @Override
        public void run() {
            try {
                //read data from the subscribed topics
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> rec : records) {
                        log.info("Topic:" + rec.topic() +
                                " Key:" + rec.key() +
                                " Value:" + rec.value() +
                                " Partition:" + rec.partition() +
                                " offset:" + rec.offset());
                    }
                }
            } catch(WakeupException e){
                log.info("Shutting down!");
            }finally {
                consumer.close();
                //this will signal the main thread that this thread is done it's  work
                latch.countDown();
            }
        }

        public void shutdown(){
            // throws WakeupException to interrupt consumer.poll() method
            consumer.wakeup();
        }
    }
}
