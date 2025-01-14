package com.github.rampi;

import com.github.rampi.state.BoundedMemoryRocksDBConfig;
import com.github.rampi.topology.MyPAPITopology;
import io.confluent.common.utils.Utils;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Properties;
import java.util.Set;

@Log
public class MyPapiApp extends LogAndContinueExceptionHandler {

    final static String APP_NAME = "my-kafka-streams-app-papi";

    @SneakyThrows
    public static void main(String[] args) {
        log.info("My stateful KStream App!");

        // count maximum number of views for each product every 5 minutes

        // Set up configuration properties for the Kafka Streams application
        Properties props = Utils.loadProps("client.properties");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put("default.deserialization.exception.handler", MyPapiApp.class.getName());

        // rocksdb tuning
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedMemoryRocksDBConfig.class.getName());
        props.put(BoundedMemoryRocksDBConfig.TOTAL_OFF_HEAP_SIZE_MB, "3000");
        props.put(BoundedMemoryRocksDBConfig.TOTAL_MEMTABLE_MB, "300");

        // Create topics if they do not exist
        AdminClient admin = KafkaAdminClient.create(props);
        admin.createTopics(
                Set.of(
                        new NewTopic("shoestore_clickstream", 1, (short) 3)
                )
        );

        Topology topology = MyPAPITopology.build(props);

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

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.severe("Deserialization exception: " + exception.getMessage());
        log.severe("Cause: " + exception.getCause().toString());
        return super.handle(context, record, exception);
    }
}