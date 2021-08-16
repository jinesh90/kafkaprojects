package com.github.jinesh90;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumerBasic {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(consumerBasic.class);

        // utility import for constant
        Utils utils = new Utils();


        // create properties.
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,utils.BOOTSTRAP_SERVERS);
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"1");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // create consumer
        KafkaConsumer <String,String> consumer = new KafkaConsumer<String, String>(consumerProps);

        // subscribe consumer
        consumer.subscribe(Arrays.asList("first_topic","second_topic"));

        // poll for new data.
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records) {
                logger.info("Record Key: "+ record.key() + "\n" +
                        "Record Value: "+ record.value() + "\n" +
                        "Record Partiton: "+ record.partition() + "\n" +
                        "Record Offset: "+ record.offset());

            }

        }


    }
}
