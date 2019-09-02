package in.jimbo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class App {

    public static void main( String[] args ) throws InterruptedException {
        System.out.println( "Hello World!" );

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "144.76.119.111:9092,144.76.24.115:9092,5.9.110.169:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "debug");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PageDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<Long, Page> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList("pages"));

        while (true) {
            System.out.println("pooling...");
            ConsumerRecords<Long, Page> consumerRecords = consumer.poll(Duration.ofMillis(100));
            System.out.println("size : " + consumerRecords.count());
            for (ConsumerRecord<Long, Page> record : consumerRecords) {
                System.out.println(record.value().getH1List());
                System.out.println(record.value().getPlainTextList());
            }
            Thread.sleep(5000);
        }
    }

}

