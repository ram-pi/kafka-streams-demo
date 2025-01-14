package com.github.rampi.serdes;

import com.github.rampi.model.ShoestoreClickstream;
import com.github.rampi.model.ShoestoreClickstreamEnriched;
import com.github.rampi.model.ShoestoreShoe;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CustomSerdes {

    public static KafkaJsonSchemaSerde<ShoestoreClickstream> shoestoreClickstreamSerde(Properties envProps) {
        final KafkaJsonSchemaSerde<ShoestoreClickstream> jsonSchemaSerde = new KafkaJsonSchemaSerde<>();
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, ShoestoreClickstream.class.getName());
        serdeConfig.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        jsonSchemaSerde.configure(serdeConfig, false);
        return jsonSchemaSerde;
    }

    public static KafkaJsonSchemaSerde<ShoestoreShoe> shoestoreShoeSerde(Properties envProps) {
        final KafkaJsonSchemaSerde<ShoestoreShoe> jsonSchemaSerde = new KafkaJsonSchemaSerde<>();
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, ShoestoreShoe.class.getName());
        serdeConfig.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        jsonSchemaSerde.configure(serdeConfig, false);
        return jsonSchemaSerde;
    }

    public static KafkaJsonSchemaSerde<ShoestoreClickstreamEnriched> shoestoreClickstreamEnrichedSerde(Properties envProps) {
        final KafkaJsonSchemaSerde<ShoestoreClickstreamEnriched> jsonSchemaSerde = new KafkaJsonSchemaSerde<>();
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, ShoestoreClickstreamEnriched.class.getName());
        serdeConfig.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        jsonSchemaSerde.configure(serdeConfig, false);
        return jsonSchemaSerde;
    }
}
