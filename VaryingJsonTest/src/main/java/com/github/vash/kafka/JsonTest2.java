package com.github.vash.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.log4j.Logger;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;


public class JsonTest2 {
    private final static Logger log = Logger.getLogger(JsonTest2.class);
    public static void main(String[] args) {
        log.info("producer2 is starting");
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");


        String topic = "Json_test";
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> JsonSerde = Serdes.serdeFrom(jsonSerializer,jsonDeserializer);
        KafkaProducer<String,JsonNode> kafkaProducer = new KafkaProducer<String, JsonNode>(properties);
        Integer i = 1;
        Integer Key = 1000;
        Random rand = new Random();
        while(i<10000000)
        {

            ObjectNode FirstJson = JsonNodeFactory.instance.objectNode();
            String name = "anotherproducer"+i.toString();
            Key = Key + 1;
            FirstJson.put("name",name);
            Instant now = Instant.now();
            Integer num = Math.abs(rand.nextInt(150) + 10);
            FirstJson.put("time", now.toString());
            FirstJson.put("Amount", num);
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode  jsonNode = objectMapper.valueToTree(FirstJson.toString());
            log.info(jsonNode.toString());

            kafkaProducer.send(new ProducerRecord<String,JsonNode>(topic, Key.toString(), jsonNode));
            i = i + 1;
            Key = Key + 1;
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }


        }

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
