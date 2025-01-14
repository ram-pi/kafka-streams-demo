package com.github.rampi.papi;

import com.github.rampi.model.ShoestoreClickstream;
import lombok.extern.java.Log;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.time.Duration;
import java.util.Date;
import java.util.Set;

@Log
public class MyProcessorSupplier implements ProcessorSupplier<String, ShoestoreClickstream, String, Long> {

    final String storeName;

    public MyProcessorSupplier(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return ProcessorSupplier.super.stores();
    }

    public Processor<String, ShoestoreClickstream, String, Long> get() {
        return new Processor<String, ShoestoreClickstream, String, Long>() {

            private ProcessorContext<String, Long> context;
            private KeyValueStore<String, Long> store;


            @Override
            public void process(Record<String, ShoestoreClickstream> record) {
                String key = "TOTAL";
                Long currentVT = store.get(key);
                if (currentVT == null)
                    currentVT = 0L;

                currentVT += 1;
                //store.put(record.key(), currentVT);
                store.put(key, currentVT);
                context.forward(new Record<>(key, currentVT, record.timestamp()));
            }

            @Override
            public void init(ProcessorContext<String, Long> context) {
                this.context = context;
                this.store = context.getStateStore(storeName);

                // punctuation
                this.context.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, this::streamTimePunctuator);
                this.context.schedule(Duration.ofHours(1), PunctuationType.WALL_CLOCK_TIME, this::wallClockTimePunctuator);
            }

            void wallClockTimePunctuator(Long timestamp) {
                log.info("@" + new Date(timestamp) + " wallClockTimePunctuator runnnig...");
                try {
                    store.put("TOTAL", 0L);
                    log.info("Reset all view-times to zero");
                } catch (Exception e) {
                    log.info("Error: " + e.getMessage());
                }
            }

            void streamTimePunctuator(Long timestamp) {
                log.info("@" + new Date(timestamp) + " streamTimePunctuator runnnig...");
            }
        };
    }
}
