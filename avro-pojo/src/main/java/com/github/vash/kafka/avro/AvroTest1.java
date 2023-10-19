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
import com.github.vash.kafka.avro.types.One_avro_avro_1;

import java.time.Instant;
import java.util.Properties;


public class AvroTest1 {
    private final static Logger log = Logger.getLogger(AvroTest1.class);
    public static void main(String[] args) {
        log.info("producing 1 is starting");
        Properties properties = new Properties();
        //properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");


        String topic = "avro-test_21";
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> JsonSerde = Serdes.serdeFrom(jsonSerializer,jsonDeserializer);
        KafkaProducer<String,One_avro_avro_1> kafkaProducer = new KafkaProducer<>(properties);
        Integer i = 1;
        Integer Key = 1000;
        while(i<10000000)
        {

            //ObjectNode FirstJson = JsonNodeFactory.instance.objectNode();
            One_avro_avro_1 FirstJson = new One_avro_avro_1();
            String first_name = "production"+i.toString();
            Key = Key + 1;
            //FirstJson.put("name",name);
            FirstJson.setFirstName(first_name);

            Instant now = Instant.now();
            //FirstJson.put("time", now.toString());
            FirstJson.setTime(now.toEpochMilli());
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode  jsonNode = objectMapper.valueToTree(FirstJson.toString());
            //log.info(jsonNode.toString());

            kafkaProducer.send(new ProducerRecord<String, One_avro_avro_1>(topic, Key.toString(), FirstJson),
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
