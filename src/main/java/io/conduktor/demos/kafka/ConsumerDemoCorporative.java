package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCorporative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        String bootstrapServer = "localhost:9092";
        String groupId = "Java2";
        String topic = "JavaApp";

        //Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        // if we want to make a consumer static --> properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "");
        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //get a Refference to the current thread
        final Thread mainThread = Thread.currentThread();
        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shutdown, Let's exit by calling consumer wakeup...!!");
                consumer.wakeup();

                //join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });
        try{
            //Subscribe Consumer to our topics
            //consumer.subscribe(Collections.singletonList(topic)); --> For Single subscription

            //Multi Subscription
            consumer.subscribe(Arrays.asList(topic));

            //Poll for new data
            while (true){

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: "+record.key()+" ,value: "+record.value());
                    log.info("Partition: "+record.partition()+" ,Offset: "+record.offset());
                }
            }

        }catch (WakeupException w){
            log.info("Wake up exception!");
            //We can ignore this exception when closing a consumer
        }catch (Exception ex){
            log.error("Unexpected Exception");
        }finally {
            consumer.close();
            log.info("The consumer is now successfully closed.");
        }
    }
}
