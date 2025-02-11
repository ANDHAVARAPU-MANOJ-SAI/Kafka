package org.example.MultiServerKafka;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConsumerKafka1 {

    public static void main(String[] args){
        KafkaConsumer kafkaConsumer1=null;
        Logger logger = LogManager.getLogger(ConsumerKafka1.class);
        kafkaConsumer1 = ConfigurationProducerConsumer.configConsumer(kafkaConsumer1, "Group1", "localhost:9092, localhost:9093", logger);
        ConsumerMain.subscribe(logger, kafkaConsumer1, "topicA");
        ConsumerMain.getMessage(logger, kafkaConsumer1);
        ConsumerMain.close(kafkaConsumer1);

    }

}