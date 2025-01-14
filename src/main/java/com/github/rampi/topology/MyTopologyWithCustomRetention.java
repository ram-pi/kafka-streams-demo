package com.github.rampi.topology;

import com.github.rampi.model.ShoestoreClickstream;
import com.github.rampi.serdes.CustomSerdes;
import lombok.extern.java.Log;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;

@Log
public class MyTopologyWithCustomRetention {

    public static String STATE_STORE_NAME = "my_state_store";

    public static Topology build(Properties props) {
        // Create a StreamsBuilder object to build the topology of the Kafka Streams application
        StreamsBuilder builder = new StreamsBuilder();
        StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(STATE_STORE_NAME), Serdes.String(), Serdes.Long());
        builder.addStateStore(storeBuilder);

        // Source streams
        KStream<String, ShoestoreClickstream> clicks = builder.stream("shoestore_clickstream", Consumed.with(Serdes.String(), CustomSerdes.shoestoreClickstreamSerde(props))).selectKey(
                (key, value) -> value.getProductId()
        );


        // TimeWindow Definition - 10 Minutes
        TimeWindows timeWindows =
                TimeWindows.ofSizeAndGrace(java.time.Duration.ofMinutes(10), java.time.Duration.ofMinutes(1))
                        .advanceBy(Duration.ofMinutes(1));

        // Materialized definition - Used to describe how a StateStore should be materialized - Retention 30 minutes
        Materialized<String, Long, WindowStore<Bytes, byte[]>> asWindowWithRetention =
                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("state_store_02")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                        .withRetention(Duration.ofMinutes(30));

        clicks
                .groupByKey()
                .windowedBy(timeWindows)
                .count(asWindowWithRetention)
                .toStream()
                .to("my_test_with_custom_retention");

        // Build the topology of the Kafka Streams application
        return builder.build();
    }
}
