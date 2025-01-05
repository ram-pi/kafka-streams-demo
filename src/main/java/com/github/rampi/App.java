package com.github.rampi;

import com.github.rampi.model.ShoestoreClickstream;
import com.github.rampi.model.ShoestoreClickstreamEnriched;
import com.github.rampi.model.ShoestoreShoe;
import io.confluent.common.utils.Utils;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Log
public class App extends LogAndContinueExceptionHandler {

    @SneakyThrows
    public static void main(String[] args) {
        log.info("My stateful KStream App!");

        // This kafka stream app will read from shoestore_clickstream topic and calculate how many views each product has every 5 minutes
        // The result will be written to shoestore_clickstream_product_views topic
        // The result will be a KTable with the following schema:
        // key: product_id
        // value: number of views

        // Set up configuration properties for the Kafka Streams application
        Properties props = Utils.loadProps("client.properties");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-kafka-streams-app");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put("default.deserialization.exception.handler", App.class.getName());

        // Create topics if they do not exist
        AdminClient admin = KafkaAdminClient.create(props);
        admin.createTopics(
                Set.of(
                        new NewTopic("shoestore_clickstream", 6, (short) 3),
                        new NewTopic("shoestore_shoe", 6, (short) 3),
                        new NewTopic("shoestore_clickstream_product_views", 6, (short) 3).configs(Map.of("cleanup.policy", "compact"))
                )
        );

        // Json Schema Serde
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", props.get("schema.registry.url"));

        // ShoestoreClickstream serde
        serdeConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, ShoestoreClickstream.class.getName());
        KafkaJsonSchemaSerde<ShoestoreClickstream> shoestoreClickstreamKafkaJsonSchemaSerde = new KafkaJsonSchemaSerde<>();
        shoestoreClickstreamKafkaJsonSchemaSerde.configure(serdeConfig, false);
        // ShoestoreShoe serde
        serdeConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, ShoestoreShoe.class.getName());
        KafkaJsonSchemaSerde<ShoestoreShoe> shoestoreShoeKafkaJsonSchemaSerde = new KafkaJsonSchemaSerde<>();
        shoestoreShoeKafkaJsonSchemaSerde.configure(serdeConfig, false);
        // ShoestoreClickstreamEnriched serde
        serdeConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, ShoestoreClickstreamEnriched.class.getName());
        KafkaJsonSchemaSerde<ShoestoreClickstreamEnriched> shoestoreClickstreamEnrichedKafkaJsonSchemaSerde = new KafkaJsonSchemaSerde<>();
        shoestoreClickstreamEnrichedKafkaJsonSchemaSerde.configure(serdeConfig, false);

        // Create a StreamsBuilder object to build the topology of the Kafka Streams application
        StreamsBuilder builder = new StreamsBuilder();

        // Source streams
        KStream<String, ShoestoreClickstream> clicks = builder.stream("shoestore_clickstream", Consumed.with(Serdes.String(), shoestoreClickstreamKafkaJsonSchemaSerde)).selectKey(
                (key, value) -> value.getProductId()
        );
        clicks.peek((key, value) -> log.info("Click: " + value + " with key: " + key));

        KTable<String, ShoestoreShoe> shoes = builder.stream("shoestore_shoe", Consumed.with(Serdes.String(), shoestoreShoeKafkaJsonSchemaSerde)).selectKey(
                (key, value) -> value.getId()
        ).toTable(Named.as("shoestore_shoe_table"));
        shoes.toStream().peek((key, value) -> log.info("Shoe: " + value + " with key: " + key));

        // Click Shoe join
        KStream<String, ShoestoreClickstreamEnriched> clicksEnriched = clicks.join(shoes, (click, shoe) -> {
            ShoestoreClickstreamEnriched res = new ShoestoreClickstreamEnriched();
            if (shoe != null)
                res.setProductName(shoe.getName());
            else
                log.severe("Shoe not found for click: " + click.getProductId());

            res.setViewTime(click.getViewTime());
            res.setUserId(click.getUserId());
            res.setIp(click.getIp());
            res.setPageUrl(click.getPageUrl());
            res.setProductId(click.getProductId());
            res.setTs(click.getTs());
            return res;
        });
        clicksEnriched.to("shoestore_clickstream_enriched", Produced.with(Serdes.String(), shoestoreClickstreamEnrichedKafkaJsonSchemaSerde));

        // Clicks per product every 5 minutes
        KTable<Windowed<String>, Long> productViews =
                clicksEnriched
                        .selectKey(
                                (key, value) -> value.getProductName()
                        )
                        .groupByKey(Grouped.with(Serdes.String(), shoestoreClickstreamEnrichedKafkaJsonSchemaSerde))
                        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(10), Duration.ofMinutes(1)))
                        .count();

        productViews.toStream()
                .map((key, value) -> new KeyValue<>(windowedKeyToString(key), value))
                .peek(
                        (key, value) -> log.info("Product: " + key + " with views: " + value)
                )
                .to("shoestore_clickstream_product_views", Produced.with(Serdes.String(), Serdes.Long()));

        // Build the topology of the Kafka Streams application
        Topology topology = builder.build();

        // Print the topology of the Kafka Streams application to the console
        log.info("Topology: " + topology.describe());

        // Create a KafkaStreams object to start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler(
                (Thread thread, Throwable throwable) -> {
                    log.severe("Uncaught exception: " + throwable.getMessage());
                    log.severe("Cause: " + throwable.getCause().toString());
                }
        );

        // Stop the Kafka Streams application gracefully when the JVM is shut down
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // state listener
        streams.setStateListener((newState, oldState) -> {
            log.info("State changed from: " + oldState.name() + " to: " + newState.name());
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                Set<ThreadMetadata> threadMetadata = streams.metadataForLocalThreads();
                for (ThreadMetadata threadMetadatum : threadMetadata) {
                    log.info(
                            "ACTIVE_TASKS: " + threadMetadatum.activeTasks().size() +
                                    " STANDBY_TASKS: " + threadMetadatum.standbyTasks().size()

                    );
                }
            }
        });

        // Clean up the Kafka Streams application before starting
        streams.cleanUp();

        // Start the Kafka Streams application
        streams.start();
    }

    public static String windowedKeyToString(Windowed<String> key) {
        // transform epoch to human readable date
        return String.format("[%s@%s/%s]", key.key(), Instant.ofEpochMilli(key.window().start()), Instant.ofEpochMilli(key.window().end()));
    }

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.severe("Deserialization exception: " + exception.getMessage());
        log.severe("Cause: " + exception.getCause().toString());
        return super.handle(context, record, exception);
    }
}
