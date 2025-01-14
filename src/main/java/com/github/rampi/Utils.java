package com.github.rampi;

import org.apache.kafka.streams.kstream.Windowed;

import java.time.Instant;

public class Utils {

    public static String windowedKeyToString(Windowed<String> key) {
        // transform epoch to human readable date
        return String.format("[%s@%s/%s]", key.key(), Instant.ofEpochMilli(key.window().start()), Instant.ofEpochMilli(key.window().end()));
    }
}
