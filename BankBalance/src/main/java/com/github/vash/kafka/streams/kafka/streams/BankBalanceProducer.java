package com.github.vash.kafka.streams.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class BankBalanceProducer {
    public static void main(String[] args) {
        final Logger logger = Logger.getLogger(BankBalanceProducer.class);
        String names_array [] = {"ram","sham","derek","vikram","prabha","SATE"};
        String bootstrapServers = "localhost:9092";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");


        String topic = "input_bank_balance";
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String,String>(properties);

        Integer count_of_messages = 0;
        Integer min = 0;
        Random rand = new Random();
        while(min<10000) {
            while (count_of_messages < 100) {
                for (Integer j = 0; j < names_array.length; j++) {
                    String key = names_array[j];
                    Integer num = Math.abs(rand.nextInt(150) + 10);

                    //HashMap<Integer,String> map=new HashMap<Integer,String>();//Creating HashMap
                    //kafkaProducer.send(new ProducerRecord<>(topic,key,));
                    ObjectMapper mapper = new ObjectMapper();
                    ArrayNode arrayNode = mapper.createArrayNode();
                    ObjectNode objectNode1 = mapper.createObjectNode();
                    Instant instant = Instant.now();
                    //ObjectNode objectNode1 = mapper.createObjectNode();
                    objectNode1.put("Name", key);
                    objectNode1.put("Amount", num);
                    objectNode1.put("time", instant.toString());
                    //arrayNode.add(objectNode1);
                    System.out.println(objectNode1.toString());

                    kafkaProducer.send(new ProducerRecord<>(topic, key, objectNode1.toString()));
                    count_of_messages = count_of_messages + 1;

                }
                logger.info("value in run is " + count_of_messages);
            }
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            min = min + 1;
            count_of_messages = 0;


        }
        kafkaProducer.flush();
        kafkaProducer.close();






    }
}
