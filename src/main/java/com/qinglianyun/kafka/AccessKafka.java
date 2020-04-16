package com.qinglianyun.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ Author ：
 * @ Company: qinglian cloud
 * @ Date   ：Created in
 * @
 */
public class AccessKafka {

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "139.217.102.185:6667,139.217.83.148:6667,139.217.103.240:6667");
        prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> out = new KafkaProducer<>(prop);
        ProducerRecord<String, String> record = new ProducerRecord<>("test", null, "java access");
        out.send(record);
        out.flush();
        out.close();
    }
}
