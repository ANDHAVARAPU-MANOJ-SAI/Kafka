package org.example.TwoServerKafka;

import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafka {
    private KafkaConsumer kafkaConsumer;

    public void start(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"Demo");
        kafkaConsumer = new KafkaConsumer<>(properties);
    }

    public void subscribe(){
        kafkaConsumer.subscribe(Arrays.asList("test-topic"));
    }

    public void close(){
        kafkaConsumer.close();
    }

    public void getMessage(Logger logger){
        while (true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> consumerRecord: consumerRecords){
                logger.info(consumerRecord.topic()+consumerRecord.key());
//                System.out.println(consumerRecord.topic());
//                System.out.println(consumerRecord.value());
//                System.out.println(consumerRecord.offset());
            }
        }
    }

    public static void main(String[] args){
        org.example.OneServerKafka.ConsumerKafka consumerKafka = new org.example.OneServerKafka.ConsumerKafka();
        Logger logger= LogManager.getLogger(org.example.OneServerKafka.ConsumerKafka.class);
        consumerKafka.start();
        consumerKafka.subscribe();
        consumerKafka.getMessage(logger);
        consumerKafka.close();

    }
}