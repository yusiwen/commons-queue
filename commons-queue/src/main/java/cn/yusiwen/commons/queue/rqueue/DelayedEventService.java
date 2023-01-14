package cn.yusiwen.commons.queue.rqueue;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import static io.lettuce.core.SetArgs.Builder.ex;
import static io.lettuce.core.ZAddArgs.Builder.nx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import javax.annotation.PreDestroy;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.yusiwen.commons.queue.rqueue.context.EventContextHandler;
import cn.yusiwen.commons.queue.rqueue.context.NoopEventContextHandler;
import cn.yusiwen.commons.queue.rqueue.metrics.Metrics;
import cn.yusiwen.commons.queue.rqueue.metrics.NoopMetrics;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.Value;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * @author Siwen Yu
 * @since 1.0.0
 */
public class DelayedEventService implements Closeable {

    public static final class Builder {
        /**
         * RedisClient
         */
        private RedisClient client;

        /**
         * ObjectMapper
         */
        private ObjectMapper mapper = new ObjectMapper();
        /**
         * Polling timeout
         */
        private Duration pollingTimeout = Duration.ofSeconds(1);
        /**
         * Enable scheduling or not
         */
        private boolean enableScheduling = true;
        /**
         * Scheduling interval
         */
        private Duration schedulingInterval = Duration.ofMillis(500);
        /**
         * Scheduling batch size
         */
        private int schedulingBatchSize = 100;
        /**
         * Scheduler
         */
        private Scheduler scheduler = Schedulers.elastic();
        /**
         * Retry attempt times
         */
        private int retryAttempts = 70;
        /**
         * Metrics
         */
        private Metrics metrics = new NoopMetrics();
        /**
         * EventContextHandler
         */
        private EventContextHandler eventContextHandler = new NoopEventContextHandler();
        /**
         * Prefix for dataset
         */
        private String dataSetPrefix = "de_";
        /**
         * Subscription refreshing interval
         */
        private Duration refreshSubscriptionInterval;

        private Builder() {}

        @NotNull
        public Builder mapper(@NotNull ObjectMapper val) {
            mapper = val;
            return this;
        }

        public Builder client(@NotNull RedisClient val) {
            client = val;
            return this;
        }

        public Builder pollingTimeout(@NotNull Duration val) {
            pollingTimeout = val;
            return this;
        }

        Builder enableScheduling(boolean val) {
            enableScheduling = val;
            return this;
        }

        public Builder schedulingInterval(@NotNull Duration val) {
            schedulingInterval = val;
            return this;
        }

        public Builder retryAttempts(int val) {
            retryAttempts = val;
            return this;
        }

        public Builder handlerScheduler(@NotNull Scheduler val) {
            scheduler = val;
            return this;
        }

        public Builder metrics(@NotNull Metrics val) {
            metrics = val;
            return this;
        }

        public Builder eventContextHandler(@NotNull EventContextHandler val) {
            eventContextHandler = val;
            return this;
        }

        public Builder dataSetPrefix(@NotNull String val) {
            dataSetPrefix = val;
            return this;
        }

        public Builder schedulingBatchSize(int val) {
            schedulingBatchSize = val;
            return this;
        }

        public Builder refreshSubscriptionsInterval(@NotNull Duration val) {
            refreshSubscriptionInterval = val;
            return this;
        }

        public DelayedEventService build() {
            return new DelayedEventService(this);
        }
    }

    private static class HandlerAndSubscription<T extends Event> {
        /**
         * Class
         */
        private final Class<T> type;
        /**
         * Handler
         */
        private final Function<T, Mono<Boolean>> handler;
        /**
         * Parallelism
         */
        private final int parallelism;
        /**
         * Subscription
         */
        private final Disposable subscription;

        private HandlerAndSubscription(Class<T> type, Function<T, Mono<Boolean>> handler, int parallelism,
            Disposable subscription) {
            this.type = type;
            this.handler = handler;
            this.parallelism = parallelism;
            this.subscription = subscription;
        }
    }

    /**
     * Delimiter
     */
    private static final String DELIMITER = "###";
    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(DelayedEventService.class);

    /**
     * Subscription
     */
    private final Map<Class<? extends Event>, HandlerAndSubscription<? extends Event>> subscriptions =
        new ConcurrentHashMap<>();

    /**
     * ObjectMapper
     */
    private final ObjectMapper mapper;
    /**
     * RedisClient
     */
    private final RedisClient client;
    /**
     * Polling timeout
     */
    private final Duration pollingTimeout;
    /**
     * Retry attempt times
     */
    private final int retryAttempts;
    /**
     * Lock timeout
     */
    private final Duration lockTimeout;

    /**
     * Scheduler
     */
    private final Scheduler handlerScheduler;
    /**
     * Dispatch redis commands
     */
    private final RedisCommands<String, String> dispatchCommands;
    /**
     * Metrics redis commands
     */
    private final RedisCommands<String, String> metricsCommands;
    /**
     * Reactive redis commands
     */
    private final RedisReactiveCommands<String, String> reactiveCommands;
    /**
     * Single thread scheduler
     */
    private final Scheduler single = Schedulers.newSingle("redis-single");
    /**
     * RedisClient
     */
    private final ScheduledThreadPoolExecutor dispatcherExecutor =
        new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            /**
             * Thread number
             */
            final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(@NotNull Runnable r) {
                return new Thread(r, "schedule-dispatcher" + threadNumber.getAndIncrement());
            }
        });
    /**
     * Metrics
     */
    private final Metrics metrics;
    /**
     * EventContextHandler
     */
    private final EventContextHandler contextHandler;
    /**
     * Prefix for dataset
     */
    private final String dataSetPrefix;
    /**
     * Scheduling batch size
     */
    private final Limit schedulingBatchSize;

    /**
     * Name for zset
     */
    private final String zsetName;
    /**
     * Key for lock
     */
    private final String lockKey;
    /**
     * HashSet for metadata
     */
    private final String metadataHset;

    private DelayedEventService(Builder builder) {
        mapper = requireNonNull(builder.mapper, "object mapper");
        client = requireNonNull(builder.client, "redis client");
        contextHandler = requireNonNull(builder.eventContextHandler, "event context handler");
        pollingTimeout = checkNotShorter(builder.pollingTimeout, Duration.ofMillis(50), "polling interval");
        lockTimeout = Duration.ofSeconds(2);
        retryAttempts = checkInRange(builder.retryAttempts, 1, 100, "retry attempts");
        handlerScheduler = requireNonNull(builder.scheduler, "scheduler");
        metrics = requireNonNull(builder.metrics, "metrics");
        dataSetPrefix = requireNonNull(builder.dataSetPrefix, "data set prefix");
        schedulingBatchSize = Limit.from(checkInRange(builder.schedulingBatchSize, 1, 1000, "scheduling batch size"));

        zsetName = dataSetPrefix + "delayed_events";
        lockKey = dataSetPrefix + "delayed_events_lock";
        metadataHset = dataSetPrefix + "events";

        dispatchCommands = client.connect().sync();
        metricsCommands = client.connect().sync();
        reactiveCommands = client.connect().reactive();

        if (builder.enableScheduling) {
            long schedulingInterval =
                checkNotShorter(builder.schedulingInterval, Duration.ofMillis(50), "scheduling interval").toNanos();
            dispatcherExecutor.scheduleWithFixedDelay(this::dispatchDelayedMessages, schedulingInterval,
                schedulingInterval, NANOSECONDS);
        }

        if (builder.refreshSubscriptionInterval != null) {
            long refreshInterval = checkNotShorter(builder.refreshSubscriptionInterval, Duration.ofMinutes(5),
                "refresh subscription interval").toNanos();
            dispatcherExecutor.scheduleWithFixedDelay(this::refreshSubscriptions, refreshInterval, refreshInterval,
                NANOSECONDS);
        }

        metrics.registerScheduledCountSupplier(() -> metricsCommands.zcard(zsetName));
    }

    void refreshSubscriptions() {
        subscriptions.replaceAll((k, v) -> {
            v.subscription.dispose();
            return createNewSubscription(v);
        });
    }

    @NotNull
    public static Builder delayedEventService() {
        return new Builder();
    }

    public <T extends Event> boolean removeHandler(@NotNull Class<T> eventType) {
        requireNonNull(eventType, "event type");

        HandlerAndSubscription<? extends Event> subscription = subscriptions.remove(eventType);
        if (subscription != null) {
            subscription.subscription.dispose();
            return true;
        }
        return false;
    }

    public <T extends Event> void addHandler(@NotNull Class<T> eventType,
        @NotNull Function<@NotNull T, @NotNull Mono<Boolean>> handler, int parallelism) {
        requireNonNull(eventType, "event type");
        requireNonNull(handler, "handler");
        checkInRange(parallelism, 1, 100, "parallelism");

        subscriptions.computeIfAbsent(eventType, re -> {
            InnerSubscriber<T> subscription = createSubscription(eventType, handler, parallelism);
            metrics.registerReadyToProcessSupplier(eventType, () -> metricsCommands.llen(toQueueName(eventType)));
            return new HandlerAndSubscription<>(eventType, handler, parallelism, subscription);
        });
    }

    public Mono<Void> enqueue(@NotNull Event event, @NotNull Duration delay) {
        requireNonNull(event, "event");
        requireNonNull(delay, "delay");
        requireNonNull(event.getId(), "event id");

        return Mono.subscriberContext().flatMap(ctx -> enqueueInner(event, delay, contextHandler.eventContext(ctx)))
            .then();
    }

    @Override
    @PreDestroy
    public void close() {
        LOG.debug("shutting down delayed queue service");

        dispatcherExecutor.shutdownNow();
        subscriptions.forEach((k, v) -> v.subscription.dispose());
        single.dispose();

        dispatchCommands.getStatefulConnection().close();
        reactiveCommands.getStatefulConnection().close();
        metricsCommands.getStatefulConnection().close();

        handlerScheduler.dispose();
    }

    private <T extends Event> HandlerAndSubscription<T> createNewSubscription(HandlerAndSubscription<T> old) {
        LOG.info("refreshing subscription for [{}]", old.type.getName());
        return new HandlerAndSubscription<>(old.type, old.handler, old.parallelism,
            createSubscription(old.type, old.handler, old.parallelism));
    }

    private Mono<TransactionResult> enqueueInner(Event event, Duration delay, Map<String, String> context) {
        return executeInTransaction(() -> {
            String key = getKey(event);
            String rawEnvelope = serialize(EventEnvelope.create(event, context));

            reactiveCommands.hset(metadataHset, key, rawEnvelope).subscribeOn(single).subscribe();
            reactiveCommands.zadd(zsetName, nx(), System.currentTimeMillis() + delay.toMillis(), key)
                .subscribeOn(single).subscribe();
        }).doOnNext(v -> metrics.incrementEnqueueCounter(event.getClass()));
    }

    void dispatchDelayedMessages() {
        LOG.debug("delayed events dispatch started");

        try {
            String lock = tryLock();

            if (lock == null) {
                LOG.debug("unable to obtain lock for delayed events dispatch");
                return;
            }

            List<String> tasksForExecution = dispatchCommands.zrangebyscore(zsetName,
                Range.create(-1, System.currentTimeMillis()), schedulingBatchSize);

            if (null == tasksForExecution) {
                return;
            }

            tasksForExecution.forEach(this::handleDelayedTask);
        } catch (RedisException e) {
            LOG.warn("Error during dispatch", e);
            dispatchCommands.reset();
            throw e;
        } finally {
            unlock();
            LOG.debug("delayed events dispatch finished");
        }
    }

    private <T extends Event> InnerSubscriber<T> createSubscription(Class<T> eventType,
        Function<T, Mono<Boolean>> handler, int parallelism) {
        StatefulRedisConnection<String, String> pollingConnection = client.connect();
        InnerSubscriber<T> subscription = new InnerSubscriber<>(contextHandler, handler, parallelism, pollingConnection,
            handlerScheduler, this::removeFromDelayedQueue);
        String queue = toQueueName(eventType);

        // todo reconnect instead of reset + flux concat instead of generate sink.next(0)
        Flux.generate(sink -> sink.next(0))
            .flatMap(r -> pollingConnection.reactive().brpop(pollingTimeout.toMillis() / 1000, queue).doOnError(e -> {
                if (e instanceof RedisCommandTimeoutException) {
                    LOG.debug("polling command timed out ({} seconds)", pollingTimeout.toMillis() / 1000);
                } else {
                    LOG.warn("error polling redis queue", e);
                }
                pollingConnection.reset();
            }).onErrorReturn(KeyValue.empty(eventType.getName())), 1, // it doesn't make sense to do requests on single
                                                                      // connection in parallel
                parallelism)
            .publishOn(handlerScheduler, parallelism).defaultIfEmpty(KeyValue.empty(queue)).filter(Value::hasValue)
            .doOnNext(v -> metrics.incrementDequeueCounter(eventType)).map(Value::getValue)
            .map(v -> deserialize(eventType, v)).onErrorContinue((e, r) -> LOG.warn("Unable to deserialize [{}]", r, e))
            .subscribe(subscription);

        return subscription;
    }

    private Mono<TransactionResult> removeFromDelayedQueue(Event event) {
        return executeInTransaction(() -> {
            String key = getKey(event);
            reactiveCommands.hdel(metadataHset, key).subscribeOn(single).subscribe();
            reactiveCommands.zrem(zsetName, key).subscribeOn(single).subscribe();
        });
    }

    private Mono<TransactionResult> executeInTransaction(Runnable commands) {
        return Mono.defer(() -> {
            reactiveCommands.multi().subscribeOn(single).subscribe();
            commands.run();
            // todo reconnect instead of reset
            return reactiveCommands.exec().subscribeOn(single)
                .doOnError(e -> reactiveCommands.getStatefulConnection().reset());
        }).subscribeOn(single);
    }

    private void handleDelayedTask(String key) {
        String rawEnvelope = dispatchCommands.hget(metadataHset, key);

        // We could have stale data because other instance already processed and deleted this key
        if (rawEnvelope == null) {
            if (dispatchCommands.zrem(zsetName, key) > 0) {
                LOG.debug("key '{}' not found in HSET", key);
            }

            return;
        }

        EventEnvelope<? extends Event> currentEnvelope = deserialize(Event.class, rawEnvelope);
        EventEnvelope<? extends Event> nextEnvelope = EventEnvelope.nextAttempt(currentEnvelope);

        dispatchCommands.multi();

        if (nextEnvelope.getAttempt() < retryAttempts) {
            dispatchCommands.zadd(zsetName, nextAttemptTime(nextEnvelope.getAttempt()), key);
            dispatchCommands.hset(metadataHset, key, serialize(nextEnvelope));
        } else {
            dispatchCommands.zrem(zsetName, key);
            dispatchCommands.hdel(metadataHset, key);
        }
        dispatchCommands.lpush(toQueueName(currentEnvelope.getType()), serialize(currentEnvelope));

        dispatchCommands.exec();

        LOG.debug("dispatched event [{}]", currentEnvelope);
    }

    private long nextAttemptTime(int attempt) {
        return System.currentTimeMillis() + nextDelay(attempt) * 1000;
    }

    private long nextDelay(int attempt) {
        if (attempt < 10) { // first 10 attempts each 10 seconds
            return 10L;
        }

        if (attempt < 10 + 10) { // next 10 attempts each minute
            return 60L;
        }

        return 60L * 24; // next 50 attempts once an hour
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    private String serialize(EventEnvelope<? extends Event> envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    private <T extends Event> EventEnvelope<T> deserialize(Class<T> eventType, String rawEnvelope) {
        JavaType envelopeType = mapper.getTypeFactory().constructParametricType(EventEnvelope.class, eventType);
        try {
            return mapper.readValue(rawEnvelope, envelopeType);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String tryLock() {
        return dispatchCommands.set(lockKey, "value", ex(lockTimeout.toMillis() * 1000).nx());
    }

    private void unlock() {
        try {
            dispatchCommands.del(lockKey);
        } catch (RedisException e) {
            dispatchCommands.reset();
        }
    }

    private String toQueueName(Class<? extends Event> cls) {
        return dataSetPrefix + cls.getSimpleName().toLowerCase();
    }

    static String getKey(Event event) {
        return event.getClass().getName() + DELIMITER + event.getId();
    }

    private static int checkInRange(int value, int min, int max, String message) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                String.format("'%s' should be inside range [%s, %s]", message, min, max));
        }
        return value;
    }

    private static Duration checkNotShorter(Duration duration, Duration ref, String msg) {
        requireNonNull(duration, "given duration should be not null");

        if (duration.compareTo(ref) < 0) {
            throw new IllegalArgumentException(
                String.format("%s with value %s should be not shorter than %s", msg, duration, ref));
        }
        return duration;
    }
}
