package com.github.rampi.state;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;

import java.util.Map;

public class BoundedMemoryRocksDBConfig implements RocksDBConfigSetter {

    public static final String TOTAL_OFF_HEAP_SIZE_MB = "rocksdb.total_offheap_size_mb";
    public static final String TOTAL_MEMTABLE_MB = "rocksdb.total_memtable_mb";

    private final static long BYTE_FACTOR = 1;
    private final static long KB_FACTOR = 1024 * BYTE_FACTOR;
    private final static long MB_FACTOR = 1024 * KB_FACTOR;

    private static org.rocksdb.Cache cache;
    private static org.rocksdb.WriteBufferManager writeBufferManager;

    @Override
    public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
        if (cache == null || writeBufferManager == null)
            initCacheOnce(configs);

        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        tableConfig.setBlockCache(cache);
        options.setWriteBufferManager(writeBufferManager);

        // … other configurations here
        // …………………………………………………………………
        // … other configurations here

        options.setTableFormatConfig(tableConfig);
    }

    @Override
    public void close(String storeName, Options options) {

    }

    public synchronized void initCacheOnce(final Map<String, Object> configs) {
        // already initialized
        if (cache != null && writeBufferManager != null)
            return;

        Long offHeapMb = Long.parseLong((String) configs.get(TOTAL_OFF_HEAP_SIZE_MB));
        Long totalMemTableMemMb = Long.parseLong((String) configs.get(TOTAL_MEMTABLE_MB));

        if (cache == null) {
            cache = new org.rocksdb.LRUCache(offHeapMb * MB_FACTOR);
        }
        if (writeBufferManager == null)
            writeBufferManager = new org.rocksdb.WriteBufferManager(totalMemTableMemMb * MB_FACTOR, cache);
    }
}