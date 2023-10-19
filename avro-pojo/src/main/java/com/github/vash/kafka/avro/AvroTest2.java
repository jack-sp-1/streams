package com.github.vash.kafka.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.log4j.Logger;
//import com.github.vash.kafka.avro.types.Two_avro_avro_2;
import com.github.vash.kafka.avro.types.Two_avro_avro_2;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;


public class AvroTest2 {
    private final static Logger log = Logger.getLogger(AvroTest2.class);
    public static void main(String[] args) {
        log.info("producer2 is starting");
        Properties properties = new Properties();
        try {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName() );
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");


        String topic = "avro-test_21";
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> JsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        KafkaProducer<String,Two_avro_avro_2> kafkaProducer = new KafkaProducer<>(properties);
        Integer i = 1;
        Integer Key = 1000;
        Random rand = new Random();
        log.info("starting 1");

            while (i < 10000000) {

                //ObjectNode SecondJson = JsonNodeFactory.instance.objectNode();
                Two_avro_avro_2 SecondJson = new Two_avro_avro_2();
                String name = "anotherproducer" + i.toString();
                Key = Key + 1;
                log.info("starting 4");

                SecondJson.setFirstName(name);
                log.info("starting 5");
                Instant now = Instant.now();
                Integer num = Math.abs(rand.nextInt(150) + 10);
                //SecondJson.put("time", now.toString());
                //SecondJson.put("Amount", num);
                SecondJson.setTime(now.toEpochMilli());
                SecondJson.setAmount(num.intValue());

                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.valueToTree(SecondJson.toString());
                log.info(jsonNode.toString());

                kafkaProducer.send(new ProducerRecord<String, Two_avro_avro_2>(topic, Key.toString(), SecondJson),
                new Callback() {
                    @Override public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null){
                            System.out.println("success");
                            System.out.println(recordMetadata.toString());
                        }
                        else{
                            e.printStackTrace();
                        }
                    }
                });
                i = i + 1;
                Key = Key + 1;
                log.info("starting 2");
                try {
                    Thread.sleep(10000);
                    log.info("starting 3");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }


            }

            kafkaProducer.flush();
            kafkaProducer.close();

        }

    catch(Exception e){
        e.printStackTrace();
        log.error(e);
    }}
}
