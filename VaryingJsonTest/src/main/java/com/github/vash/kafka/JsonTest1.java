package com.github.vash.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.vash.kafka.Serialiser.JsonNew;
import com.github.vash.kafka.Serialiser.OneJson;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;


public class JsonTest1 {
    private final static Logger log = Logger.getLogger(JsonTest1.class);
    public static void main(String[] args) {
        log.info("producing 1 is starting");
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,JsonNew.class);


        String topic = "Json_test";
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> JsonSerde = Serdes.serdeFrom(jsonSerializer,jsonDeserializer);
        KafkaProducer<String,OneJson> kafkaProducer = new KafkaProducer<>(properties);
        Integer i = 1;
        Integer Key = 1000;
        while(i<10000000)
        {

            //ObjectNode FirstJson = JsonNodeFactory.instance.objectNode();
            OneJson FirstJson = new OneJson();
            String name = "production"+i.toString();
            Key = Key + 1;
            //FirstJson.put("name",name);
            FirstJson.setName(name);
            Instant now = Instant.now();
            //FirstJson.put("time", now.toString());
            FirstJson.setTime(now.toEpochMilli());
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode  jsonNode = objectMapper.valueToTree(FirstJson.toString());
            log.info(jsonNode.toString());

            kafkaProducer.send(new ProducerRecord<String,OneJson>(topic, Key.toString(), FirstJson));
            i = i + 1;
            Key = Key + 1;
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }


        }

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
