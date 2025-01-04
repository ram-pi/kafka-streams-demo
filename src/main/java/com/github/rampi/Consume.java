package com.github.rampi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rampi.model.ShoestoreClickstream;
import io.confluent.common.utils.Utils;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import org.apache.kafka.clients.consumer.*;

import java.util.Arrays;
import java.util.Properties;

@Log
public class Consume {

    @SneakyThrows
    public static void main(String[] args) {
        log.info("My Consumer!");

        // Set up configuration properties for the Kafka Streams application
        Properties props = Utils.loadProps("client.properties");
        // generate random group id
        props.put("client.id", "my-consumer-" + System.currentTimeMillis());
        props.put("group.id", "my-consumer-group" + System.currentTimeMillis());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, ShoestoreClickstream.class.getName());

        Consumer<String, JsonNode> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("shoestore_clickstream"));

        ObjectMapper mapper = new ObjectMapper();

        try {
            while (true) {
                ConsumerRecords<String, JsonNode> records = consumer.poll(100);
                for (ConsumerRecord<String, JsonNode> record : records) {
                    ShoestoreClickstream shoestoreClickstream = mapper.convertValue(record.value(), ShoestoreClickstream.class);
                    log.info("Consumed record with key " + record.key() + " and value " + shoestoreClickstream);
                }
            }
        } finally {
            consumer.close();
        }


    }
}
