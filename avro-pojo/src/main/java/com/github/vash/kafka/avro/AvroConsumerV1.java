package com.github.vash.kafka.avro;

import com.github.vash.kafka.avro.types.One_avro_avro_1;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class AvroConsumerV1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put("group.id","avro-consumer");
        properties.put("enable.auto.commit","false");
        properties.put("auto.offset.reset","earliest");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
        properties.put("specific.avro.reader","true");

        KafkaConsumer<String, One_avro_avro_1> kafkaConsumer = new KafkaConsumer<String, One_avro_avro_1>(properties);
        String topic="avro-test_21";
        kafkaConsumer.subscribe(Collections.singleton(topic));
        while(true){
            ConsumerRecords<String,One_avro_avro_1> records= kafkaConsumer.poll(1000);
            for(ConsumerRecord<String, One_avro_avro_1> record:records){
                One_avro_avro_1 rec = record.value();
                System.out.println(rec);
            }
            kafkaConsumer.commitSync();
        }
        //kafkaConsumer.close();
    }
}
