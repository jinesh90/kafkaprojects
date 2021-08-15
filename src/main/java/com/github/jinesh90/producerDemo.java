package com.github.jinesh90;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class producerDemo {
    public static void main(String[] args) {

        // utility import for constant
        Utils utils = new Utils();

        // create Producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, utils.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","hello world");



            //send data -asynchronus
            producer.send(record
            );

            // flush data
            producer.flush();


    }
}
