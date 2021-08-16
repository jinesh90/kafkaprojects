package com.github.jinesh90;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class producerWithKeyValue {
    public static void main(String[] args) throws InterruptedException, ExecutionException
    {

        // logs for producer
        Logger logger = LoggerFactory.getLogger(producerWithKeyValue.class);


        // utility import for constant
        Utils utils = new Utils();

        // create Producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, utils.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);



        for(int i=0;i<=10;i++) {

            //create a producer record
            String key = "Id_" + i ;
            ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic",key,"hello world "+ i);
            logger.info("Key: "+ key);


            // define call back
            Callback callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("Received new metadata: \n" + "Topic: " + metadata.topic() + "\n" +
                                        "Partitions: " + metadata.partition() + "\n" +
                                        "Offset: " + metadata.offset() + "\n" +
                                        "Timestamp: " + metadata.timestamp(),
                                "Key: "+ key);
                    } else {
                        logger.error("Error in producing data: " + exception.getMessage());
                    }
                }
            };

            //send data -asynchronus
            producer.send(record, callback).get();

            // flush data
            producer.flush();

        }
    }
}
