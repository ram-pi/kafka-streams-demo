package com.github.rampi.topology;

import com.github.rampi.model.ShoestoreClickstream;
import com.github.rampi.model.ShoestoreClickstreamEnriched;
import com.github.rampi.model.ShoestoreShoe;
import com.github.rampi.serdes.CustomSerdes;
import lombok.extern.java.Log;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

@Log
public class MyTopology {

    public static Topology build(Properties props) {
        // Create a StreamsBuilder object to build the topology of the Kafka Streams application
        StreamsBuilder builder = new StreamsBuilder();

        // Source streams
        KStream<String, ShoestoreClickstream> clicks = builder.stream("shoestore_clickstream", Consumed.with(Serdes.String(), CustomSerdes.shoestoreClickstreamSerde(props))).selectKey(
                (key, value) -> value.getProductId()
        );
        clicks.peek((key, value) -> log.info("Click: " + value + " with key: " + key));

        KTable<String, ShoestoreShoe> shoes = builder.stream("shoestore_shoe", Consumed.with(Serdes.String(), CustomSerdes.shoestoreShoeSerde(props))).selectKey(
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
        clicksEnriched.to("shoestore_clickstream_enriched", Produced.with(Serdes.String(), CustomSerdes.shoestoreClickstreamEnrichedSerde(props)));

        // Clicks per product every 5 minutes
        KTable<Windowed<String>, Long> productViews =
                clicksEnriched
                        .selectKey(
                                (key, value) -> value.getProductName()
                        )
                        .groupByKey(Grouped.with(Serdes.String(), CustomSerdes.shoestoreClickstreamEnrichedSerde(props)))
                        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(10), Duration.ofMinutes(1)))
                        .count();

        productViews.toStream()
                .map((key, value) -> new KeyValue<>(com.github.rampi.Utils.windowedKeyToString(key), value))
                .peek(
                        (key, value) -> log.info("Product: " + key + " with views: " + value)
                )
                .to("shoestore_clickstream_product_views", Produced.with(Serdes.String(), Serdes.Long()));

        // Build the topology of the Kafka Streams application
        return builder.build();
    }
}
