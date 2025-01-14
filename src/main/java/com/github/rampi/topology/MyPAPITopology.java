package com.github.rampi.topology;

import com.github.rampi.model.ShoestoreClickstream;
import com.github.rampi.papi.MyProcessorSupplier;
import com.github.rampi.serdes.CustomSerdes;
import lombok.extern.java.Log;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

@Log
public class MyPAPITopology {

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

        clicks
                .process(new MyProcessorSupplier(STATE_STORE_NAME), STATE_STORE_NAME).to("my_counter", Produced.with(Serdes.String(), Serdes.Long()));

        // Build the topology of the Kafka Streams application
        return builder.build();
    }
}
