package in.jimbo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Arrays;
import java.util.Properties;

public class MyProducer {
    public static void main(String[] args) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "144.76.119.111:9092,144.76.24.115:9092,5.9.110.169:9092");
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "page_parser_client");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer .class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PageSerializer.class.getName());
        Producer<Long, Page> producer = new KafkaProducer<>(producerProperties);
        Page page = new Page();
        page.setUrl("a.com");
        page.setTitle("aaaa");
        page.setH1List(Arrays.asList(new HtmlTag("ads", "afdsfsf"), new HtmlTag("asdcasd", "fasdf")));
        System.out.println(page);
        ProducerRecord<Long, Page> record = new ProducerRecord<>("pages", page);
        System.out.println();
        System.out.println(record.value().getUrl());
        producer.send(record);
        producer.close();
    }
}
