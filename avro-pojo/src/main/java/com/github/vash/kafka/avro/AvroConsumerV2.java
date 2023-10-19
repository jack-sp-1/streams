package com.github.vash.kafka.avro;

import com.github.vash.kafka.avro.types.One_avro_avro_1;
import com.github.vash.kafka.avro.types.Two_avro_avro_2;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class AvroConsumerV2 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put("group.id","avro-consumer-2");
        properties.put("enable.auto.commit","false");
        properties.put("auto.offset.reset","earliest");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
        properties.put("specific.avro.reader","true");

        KafkaConsumer<String, Two_avro_avro_2> kafkaConsumer = new KafkaConsumer<String, Two_avro_avro_2>(properties);
        String topic="avro-test_21";
        kafkaConsumer.subscribe(Collections.singleton(topic));
        while(true){
            ConsumerRecords<String,Two_avro_avro_2> records= kafkaConsumer.poll(1000);
            for(ConsumerRecord<String, Two_avro_avro_2> record:records){
                Two_avro_avro_2 rec = record.value();
                System.out.println(rec);
            }
            kafkaConsumer.commitSync();
        }
        //kafkaConsumer.close();
    }
}
