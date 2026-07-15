package io.tibobit.normalizer.lookup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodic-refresh reference-data reader for use inside Flink operators: {@link #open()} loads
 * the full map once (failing the job if the initial load fails — never run on an empty lookup)
 * and schedules a background reload every {@code refreshIntervalMs}. A failed refresh keeps the
 * last-good snapshot and logs — reference data going briefly stale is better than dropping
 * events. The {@link Loader} is a serializable function so operators can ship a JDBC query
 * closure to the task (and tests can pass a fake).
 */
public class RefreshingLookup<K, V> implements Serializable {

    /** Loads the complete reference map. Serializable so Flink can ship the closure to tasks. */
    public interface Loader<K, V> extends Serializable {
        Map<K, V> load() throws Exception;
    }

    private static final Logger LOG = LoggerFactory.getLogger(RefreshingLookup.class);

    private final Loader<K, V> loader;
    private final long refreshIntervalMs;

    private transient volatile Map<K, V> snapshot;
    private transient ScheduledExecutorService scheduler;

    public RefreshingLookup(Loader<K, V> loader, long refreshIntervalMs) {
        this.loader = loader;
        this.refreshIntervalMs = refreshIntervalMs;
    }

    /** Call from the operator's open(): initial load (propagates failure) + refresh schedule. */
    public void open() throws Exception {
        snapshot = loader.load();
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "refreshing-lookup");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleAtFixedRate(this::refresh, refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS);
    }

    /** One reload attempt; on failure keeps the last-good snapshot. Package-private for tests. */
    void refresh() {
        try {
            snapshot = loader.load();
        } catch (Exception e) {
            LOG.warn("Reference data refresh failed; keeping last-good snapshot ({} entries)",
                    snapshot.size(), e);
        }
    }

    /** Current value for the key, or null if unknown. */
    public V get(K key) {
        return snapshot.get(key);
    }

    /** Call from the operator's close(). */
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
