package com.github.vash.kafka.streams.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.log4j.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Instant;
import java.util.Properties;

public class BankBalanceStreams {
    private static Logger log = Logger.getLogger(BankBalanceProducer.class);

    public static void main(String[] args) {
        log.info("new thing");
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"bankbalance");
        //properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> JsonSerde = Serdes.serdeFrom(jsonSerializer,jsonDeserializer);
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> textlines = builder.stream ("input_bank_balance", Consumed.with(Serdes.String(), JsonSerde));
        //log.info(textlines);
        ObjectNode initialbalance = JsonNodeFactory.instance.objectNode();
        initialbalance.put("count",0);
        initialbalance.put("balance",0);
        initialbalance.put("time", Instant.ofEpochMilli(0L).toString());

        KTable<String,JsonNode> bankBalance = textlines.groupByKey(Serialized.with(Serdes.String(), JsonSerde))
                .aggregate(
                        () -> initialbalance,
                        (key,transaction,balance) -> newBalance(transaction,balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerde)
                );
        bankBalance.toStream().to("bank-exactly-once", Produced.with(Serdes.String(), JsonSerde));

        //KStream<String,Long> userBankBalance = textlines
        //        .groupByKey()
        //        .aggregate(()->0L,
        //                (aggkey,newValue,aggValue)->aggValue+ newValue.
        //                Serdes.Long(),
        //                "aggregated-value");
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.cleanUp();
        streams.start();
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("Amount").asInt());

        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }
}
