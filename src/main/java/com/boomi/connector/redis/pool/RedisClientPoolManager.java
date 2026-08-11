package com.boomi.connector.redis.pool;

import com.boomi.util.ExecutorUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Singleton registry of shared Redis clients ({@code JedisPool} for pooled standalone,
 * {@code JedisCluster} for pooled cluster connections), keyed by {@link RedisClientSettings}.
 * Modeled on the official Boomi JMS V2 connector's {@code AdapterPoolManager}: a static map of
 * active clients plus a scheduled evictor that periodically closes the expired ones, so a client
 * whose configuration was superseded (credential rotation, setting change) or that simply fell
 * out of use dies off naturally instead of living for the JVM lifetime.
 *
 * <p>One deliberate deviation from the JMS reference: the JMS manager serializes creation and
 * eviction on a single global lock, which is safe there because adapter construction is lazy.
 * {@code JedisCluster} construction performs eager network topology discovery, so a global lock
 * would stall every unrelated acquisition behind one slow cluster build. This manager locks
 * per-settings instead; the evictor takes the same per-settings lock before closing an entry, so
 * an in-flight acquire and the evictor can never race on the same client.
 *
 * <p><b>Lock objects in {@code KEY_LOCKS} are never removed</b>, including by the evictor and by
 * {@link #closeAll()} - only {@code ACTIVE_CLIENTS} entries are removed/cleared. This is
 * deliberate: once {@code KEY_LOCKS.computeIfAbsent(settings, ...)} creates the lock object for a
 * given {@link RedisClientSettings}, that exact object is the monitor every future
 * {@link #acquire}/evictor pass for that key will synchronize on, for the rest of the JVM's life.
 * An earlier version removed the {@code KEY_LOCKS} entry alongside the {@code ACTIVE_CLIENTS}
 * entry when an evicted client was closed; that made the per-key mutual exclusion depend on a
 * subtle cross-map happens-before argument (that a thread observing the {@code KEY_LOCKS} miss
 * also observes the prior {@code ACTIVE_CLIENTS} removal) to guarantee a concurrent
 * {@link #acquire} landing in that window couldn't obtain a different lock object than the
 * evictor was holding. That argument is very likely correct given
 * {@code ConcurrentHashMap}'s documented memory-consistency effects and
 * {@code computeIfAbsent}'s per-key atomicity, but it is not something a future change to this
 * class - or a future maintainer - can verify at a glance. Never removing the lock object removes
 * the argument's premise entirely: there is only ever one lock instance per settings key, so
 * there is no window in which two different lock objects can exist for it. The cost is a
 * {@code KEY_LOCKS} entry that outlives its {@code ACTIVE_CLIENTS} entry, bounded by the number of
 * distinct {@link RedisClientSettings} ever seen in the JVM's lifetime (i.e. distinct Boomi
 * connection components times their distinct configuration states over time) - not unbounded in
 * practice.
 *
 * <p>A second deliberate deviation: the JMS reference starts its evictor in a static initializer
 * and never stops it. {@code ExecutorUtil.newScheduler} threads are non-daemon, and the Connector
 * SDK has no unload/destroy callback, so a never-stopped evictor pins the deployment's
 * classloader until Atom restart - observed on a real Atom (2026-08-11) as a previous
 * deployment's evictor still logging evictions after redeploy. This manager therefore starts the
 * evictor lazily on {@link #acquire} and stops it when a sweep finds no registered clients
 * ({@link #stopEvictorIfIdle()}): after a redeploy the old deployment gets no new acquires, its
 * clients idle out within one expiry-plus-sweep window, the next sweep stops the thread, and the
 * classloader becomes collectible. The acquire-vs-self-stop race resolves because acquire
 * registers the client in {@code ACTIVE_CLIENTS} <b>before</b> its start-check: either the
 * stopping sweep's {@code isEmpty()} (under the same {@code SCHEDULER_LOCK}) sees the new entry
 * and stays alive, or it stopped first and the acquire's start-check restarts a fresh scheduler.
 * No interleaving leaves a registered client unwatched.
 */
public final class RedisClientPoolManager {

    private static final Logger LOG = Logger.getLogger(RedisClientPoolManager.class.getName());

    /** How long an idle client (no active references) may live before the evictor closes it. */
    static final long CLIENT_EXPIRATION_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(30);

    private static final long EVICTION_INTERVAL_MINUTES = 5;

    private static final ConcurrentMap<RedisClientSettings, ManagedClient> ACTIVE_CLIENTS =
            new ConcurrentHashMap<>();
    private static final ConcurrentMap<RedisClientSettings, Object> KEY_LOCKS = new ConcurrentHashMap<>();

    /**
     * Guards the evictor's lifecycle ({@link #evictionService} start/stop). Never held while a
     * per-settings lock is taken or a client is closed, so it cannot participate in a lock cycle.
     */
    private static final Object SCHEDULER_LOCK = new Object();

    /** Live eviction scheduler, or {@code null} while stopped. Guarded by {@link #SCHEDULER_LOCK}. */
    private static ScheduledExecutorService evictionService;

    private RedisClientPoolManager() {
    }

    /**
     * Returns the shared client registered for the given settings, creating and registering one
     * via {@code clientBuilder} when absent or already closed. Increments the active-reference
     * count; callers MUST balance with {@link #release(RedisClientSettings, Object)}.
     */
    @SuppressWarnings("unchecked")
    public static <T extends AutoCloseable> T acquire(RedisClientSettings settings, Supplier<T> clientBuilder) {
        Object lock = KEY_LOCKS.computeIfAbsent(settings, k -> new Object());
        T result;
        synchronized (lock) {
            long now = System.currentTimeMillis();
            ManagedClient existing = ACTIVE_CLIENTS.get(settings);
            if (existing != null && !existing.closed) {
                existing.activeReferences.incrementAndGet();
                existing.touch(now);
                result = (T) existing.client;
            } else {
                T client = clientBuilder.get();
                ACTIVE_CLIENTS.put(settings, new ManagedClient(client, now));
                LOG.info("Registered new shared Redis client for " + settings);
                result = client;
            }
        }
        ensureEvictorRunning();
        return result;
    }

    /**
     * Releases one reference to the shared client. The client itself is never closed here - only
     * the evictor (idle expiry) or {@link #closeAll()} closes clients. The identity guard skips
     * the decrement when {@code client} is not the currently registered instance (e.g. the caller
     * held a client that {@link #closeAll()} already discarded and a new one was registered
     * since), so a stale release can never corrupt the live entry's count.
     */
    public static void release(RedisClientSettings settings, Object client) {
        ManagedClient managed = ACTIVE_CLIENTS.get(settings);
        if (managed != null && managed.client == client) {
            managed.activeReferences.decrementAndGet();
            managed.touch(System.currentTimeMillis());
        }
    }

    /**
     * Starts the eviction scheduler if it is not running. Called on every {@link #acquire} -
     * idempotent and cheap (a null check under {@link #SCHEDULER_LOCK}). Lazy start plus
     * {@link #stopEvictorIfIdle()} is what lets a superseded deployment's classloader unload: the
     * SDK has no connector-unload callback, so the thread must stop itself when there is nothing
     * left to watch, and restart when work reappears.
     */
    private static void ensureEvictorRunning() {
        synchronized (SCHEDULER_LOCK) {
            if (evictionService == null) {
                evictionService = ExecutorUtil.newScheduler("Redis Client Pool Eviction Service");
                evictionService.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        runEviction(System.currentTimeMillis());
                        stopEvictorIfIdle();
                    }
                }, EVICTION_INTERVAL_MINUTES, EVICTION_INTERVAL_MINUTES, TimeUnit.MINUTES);
            }
        }
    }

    /**
     * Shuts the evictor down when no clients remain registered; the next {@link #acquire}
     * restarts it. Package-visible so tests can drive the scheduled task's exact sequence
     * (sweep, then self-stop check) deterministically. Calling {@code shutdown()} from within
     * the scheduler's own task is legal - the in-flight run completes and no further runs fire.
     */
    static void stopEvictorIfIdle() {
        synchronized (SCHEDULER_LOCK) {
            if (evictionService != null && ACTIVE_CLIENTS.isEmpty()) {
                evictionService.shutdown();
                evictionService = null;
                LOG.info("Redis client pool evictor stopped (no registered clients)");
            }
        }
    }

    /** Test-only introspection: whether the eviction scheduler is currently running. */
    static boolean isEvictorRunning() {
        synchronized (SCHEDULER_LOCK) {
            return evictionService != null;
        }
    }

    /**
     * Closes and removes every expired client. Called by the scheduled evictor; package-visible so
     * tests can drive it deterministically with a chosen clock value. Mirrors the JMS evictor
     * body: close if expired-and-not-closed, then remove if closed. Deliberately does NOT remove
     * the {@code KEY_LOCKS} entry - see the class Javadoc for why the lock object must outlive the
     * {@code ACTIVE_CLIENTS} entry it guards.
     */
    static void runEviction(long nowMillis) {
        for (Map.Entry<RedisClientSettings, ManagedClient> entry : ACTIVE_CLIENTS.entrySet()) {
            ManagedClient managed = entry.getValue();
            Object lock = KEY_LOCKS.computeIfAbsent(entry.getKey(), k -> new Object());
            synchronized (lock) {
                try {
                    if (managed.isExpired(nowMillis) && !managed.closed) {
                        managed.closed = true;
                        managed.closeQuietly();
                        LOG.info("Closed expired idle Redis client for " + entry.getKey());
                    }
                    if (managed.closed) {
                        ACTIVE_CLIENTS.remove(entry.getKey(), managed);
                    }
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "Unable to evict Redis client for " + entry.getKey(), e);
                }
            }
        }
    }

    /**
     * Closes all shared clients regardless of references or age. For shutdown and test teardown
     * only. Contract: callers must be effectively single-threaded (no concurrent acquires) - this
     * clears {@code ACTIVE_CLIENTS} without taking the per-settings locks. {@code KEY_LOCKS} is
     * deliberately left untouched - see the class Javadoc.
     */
    public static void closeAll() {
        for (Map.Entry<RedisClientSettings, ManagedClient> entry : ACTIVE_CLIENTS.entrySet()) {
            ManagedClient managed = entry.getValue();
            managed.closed = true;
            managed.closeQuietly();
        }
        ACTIVE_CLIENTS.clear();
        stopEvictorIfIdle();
    }

    /** Number of registered shared clients. Diagnostics/monitoring. */
    public static int getActiveClientCount() {
        return ACTIVE_CLIENTS.size();
    }

    /** Active reference count for the given settings, or -1 when no client is registered. */
    static int getActiveReferences(RedisClientSettings settings) {
        ManagedClient managed = ACTIVE_CLIENTS.get(settings);
        return managed == null ? -1 : managed.activeReferences.get();
    }

    /**
     * Test-only introspection: the lock object currently registered for {@code settings}, or
     * {@code null} if {@link #acquire} has never been called for it. Package-visible so tests can
     * assert the lock object's identity is stable across an eviction/rebuild cycle - the invariant
     * this class's thread-safety now relies on (see class Javadoc).
     */
    static Object getLockObject(RedisClientSettings settings) {
        return KEY_LOCKS.get(settings);
    }
}
