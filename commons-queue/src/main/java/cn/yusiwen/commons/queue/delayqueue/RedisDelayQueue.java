package cn.yusiwen.commons.queue.delayqueue;

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

import cn.yusiwen.commons.queue.delayqueue.context.NoopTaskContextHandler;
import cn.yusiwen.commons.queue.delayqueue.context.TaskContextHandler;
import cn.yusiwen.commons.queue.delayqueue.metrics.Metrics;
import cn.yusiwen.commons.queue.delayqueue.metrics.NoopMetrics;
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
public class RedisDelayQueue implements DelayQueue, Closeable {

    /**
     * Builder for RedisDelayQueue
     */
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
         * TaskContextHandler
         */
        private TaskContextHandler taskContextHandler = new NoopTaskContextHandler();
        /**
         * Prefix for dataset
         */
        private String dataSetPrefix = "de_";
        /**
         * Subscription refreshing interval
         */
        private Duration refreshSubscriptionInterval;

        private Builder() {}

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

        public Builder enableScheduling(boolean val) {
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

        public Builder taskContextHandler(@NotNull TaskContextHandler val) {
            taskContextHandler = val;
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

        public RedisDelayQueue build() {
            return new RedisDelayQueue(this);
        }
    }

    /**
     * Handler and subscription, for task handlers management
     *
     * @param <T> Task
     */
    private static class HandlerAndSubscription<T extends Task> {
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
    private static final Logger LOG = LoggerFactory.getLogger(RedisDelayQueue.class);

    /**
     * Subscriptions map, manage every task type and its handler
     */
    private final Map<Class<? extends Task>, HandlerAndSubscription<? extends Task>> subscriptions =
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
     * Redis command for dispatching tasks
     */
    private final RedisCommands<String, String> dispatchCommands;
    /**
     * Redis command for handling metrics
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
     * Scheduler thread pool
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
     * TaskContextHandler
     */
    private final TaskContextHandler contextHandler;
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

    private RedisDelayQueue(Builder builder) {
        mapper = requireNonNull(builder.mapper, "object mapper");
        client = requireNonNull(builder.client, "redis client");
        contextHandler = requireNonNull(builder.taskContextHandler, "task context handler");
        pollingTimeout = checkNotShorter(builder.pollingTimeout, Duration.ofMillis(50), "polling interval");
        lockTimeout = Duration.ofSeconds(2);
        retryAttempts = checkInRange(builder.retryAttempts, 1, 100, "retry attempts");
        handlerScheduler = requireNonNull(builder.scheduler, "scheduler");
        metrics = requireNonNull(builder.metrics, "metrics");
        dataSetPrefix = requireNonNull(builder.dataSetPrefix, "data set prefix");
        schedulingBatchSize = Limit.from(checkInRange(builder.schedulingBatchSize, 1, 1000, "scheduling batch size"));

        zsetName = dataSetPrefix + "delayed_tasks";
        lockKey = dataSetPrefix + "delayed_tasks_lock";
        metadataHset = dataSetPrefix + "tasks";

        dispatchCommands = client.connect().sync();
        metricsCommands = client.connect().sync();
        reactiveCommands = client.connect().reactive();

        // If scheduling is enabled
        if (builder.enableScheduling) {
            long schedulingInterval =
                checkNotShorter(builder.schedulingInterval, Duration.ofMillis(50), "scheduling interval").toNanos();
            // Schedule dispatchDelayedMessages in dispatcherExecutor with schedulingInterval
            dispatcherExecutor.scheduleWithFixedDelay(this::dispatchDelayedTasks, schedulingInterval,
                schedulingInterval, NANOSECONDS);
        }

        // If subscription refreshing is enabled
        if (builder.refreshSubscriptionInterval != null) {
            long refreshInterval = checkNotShorter(builder.refreshSubscriptionInterval, Duration.ofMinutes(5),
                "refresh subscription interval").toNanos();
            // Schedule refreshSubscriptions in dispatcherExecutor with refreshInterval
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
    public static Builder redisDelayQueue() {
        return new Builder();
    }

    @Override
    public <T extends Task> boolean removeTaskHandler(@NotNull Class<T> taskType) {
        requireNonNull(taskType, "task type");

        HandlerAndSubscription<? extends Task> subscription = subscriptions.remove(taskType);
        if (subscription != null) {
            subscription.subscription.dispose();
            return true;
        }
        return false;
    }

    @Override
    public <T extends Task> void addTaskHandler(@NotNull Class<T> taskType,
        @NotNull Function<@NotNull T, @NotNull Mono<Boolean>> handler, int parallelism) {
        requireNonNull(taskType, "task type");
        requireNonNull(handler, "handler");
        checkInRange(parallelism, 1, 100, "parallelism");

        subscriptions.computeIfAbsent(taskType, re -> {
            InnerSubscriber<T> subscription = createSubscription(taskType, handler, parallelism);
            metrics.registerReadyToProcessSupplier(taskType, () -> metricsCommands.llen(toQueueName(taskType)));
            return new HandlerAndSubscription<>(taskType, handler, parallelism, subscription);
        });
    }

    @Override
    public <T extends Task> Mono<Void> enqueue(@NotNull T task, @NotNull Duration delay) {
        requireNonNull(task, "task");
        requireNonNull(delay, "delay");
        requireNonNull(task.getId(), "task id");

        return Mono.subscriberContext().flatMap(ctx -> enqueueInner(task, delay, contextHandler.taskContext(ctx)))
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

    private <T extends Task> HandlerAndSubscription<T> createNewSubscription(HandlerAndSubscription<T> old) {
        LOG.info("refreshing subscription for [{}]", old.type.getName());
        return new HandlerAndSubscription<>(old.type, old.handler, old.parallelism,
            createSubscription(old.type, old.handler, old.parallelism));
    }

    /**
     * Add task to redis ZSET
     *
     * @param task Task
     * @param delay Delay time
     * @param context Context
     * @return Mono<TransactionResult>
     */
    private Mono<TransactionResult> enqueueInner(Task task, Duration delay, Map<String, String> context) {
        return executeInTransaction(() -> {
            String key = getKey(task);
            String rawEnvelope = serialize(TaskWrapper.create(task, context));

            reactiveCommands.hset(metadataHset, key, rawEnvelope).subscribeOn(single).subscribe();
            // Use NX option: Only add new elements. Don't update already existing elements.
            reactiveCommands.zadd(zsetName, nx(), System.currentTimeMillis() + delay.toMillis(), key)
                .subscribeOn(single).subscribe();
        }).doOnNext(v -> metrics.incrementEnqueueCounter(task.getClass()));
    }

    /**
     * Task dispatcher
     */
    void dispatchDelayedTasks() {
        LOG.debug("delayed tasks dispatch started");

        try {
            String lock = tryLock();

            if (lock == null) {
                LOG.debug("unable to obtain lock for delayed tasks dispatch");
                return;
            }

            // Get keys from zset by score of current time
            List<String> tasksForExecution = dispatchCommands.zrangebyscore(zsetName,
                Range.create(-1, System.currentTimeMillis()), schedulingBatchSize);

            if (null == tasksForExecution) {
                LOG.debug("found no task to execute");
                return;
            }

            tasksForExecution.forEach(this::notify);
        } catch (RedisException e) {
            LOG.warn("Error during dispatch", e);
            dispatchCommands.reset();
            throw e;
        } finally {
            unlock();
            LOG.debug("delayed tasks dispatch finished");
        }
    }

    private <T extends Task> InnerSubscriber<T> createSubscription(Class<T> taskType,
        Function<T, Mono<Boolean>> handler, int parallelism) {
        StatefulRedisConnection<String, String> pollingConnection = client.connect();
        InnerSubscriber<T> subscription = new InnerSubscriber<>(contextHandler, handler, parallelism, pollingConnection,
            handlerScheduler, this::removeFromDelayedQueue);
        String queue = toQueueName(taskType);

        // todo reconnect instead of reset + flux concat instead of generate sink.next(0)
        Flux.generate(sink -> sink.next(0))
            // Get last element of the list in redis
            .flatMap(r -> pollingConnection.reactive().brpop(pollingTimeout.toMillis() / 1000, queue).doOnError(e -> {
                if (e instanceof RedisCommandTimeoutException) {
                    LOG.debug("polling command timed out ({} seconds)", pollingTimeout.toMillis() / 1000);
                } else {
                    LOG.warn("error polling redis queue", e);
                }
                pollingConnection.reset();
            }).onErrorReturn(KeyValue.empty(taskType.getName())), 1, // it doesn't make sense to do requests on single
                                                                     // connection in parallel
                parallelism)
            .publishOn(handlerScheduler, parallelism).defaultIfEmpty(KeyValue.empty(queue)).filter(Value::hasValue)
            .doOnNext(v -> metrics.incrementDequeueCounter(taskType)).map(Value::getValue)
            .map(v -> deserialize(taskType, v)).onErrorContinue((e, r) -> LOG.warn("Unable to deserialize [{}]", r, e))
            .subscribe(subscription);

        return subscription;
    }

    /**
     * Remove task from redis ZSET
     *
     * @param task Task
     * @param <T> Task type
     * @return Mono<TransactionResult>
     */
    private <T extends Task> Mono<TransactionResult> removeFromDelayedQueue(T task) {
        return executeInTransaction(() -> {
            String key = getKey(task);
            reactiveCommands.hdel(metadataHset, key).subscribeOn(single).subscribe();
            reactiveCommands.zrem(zsetName, key).subscribeOn(single).subscribe();
        });
    }

    /**
     * Run redis commands in one transaction
     *
     * @param commands Redis commands
     * @return Mono<TransactionResult>
     */
    private Mono<TransactionResult> executeInTransaction(Runnable commands) {
        return Mono.defer(() -> {
            reactiveCommands.multi().subscribeOn(single).subscribe();
            commands.run();
            // todo reconnect instead of reset
            return reactiveCommands.exec().subscribeOn(single)
                .doOnError(e -> reactiveCommands.getStatefulConnection().reset());
        }).subscribeOn(single);
    }

    /**
     * Notify task handlers polling on Redis lists
     *
     * @param key Key of task
     */
    private void notify(String key) {
        String rawEnvelope = dispatchCommands.hget(metadataHset, key);

        // We could have stale data because other instance already processed and deleted this key
        if (rawEnvelope == null) {
            if (dispatchCommands.zrem(zsetName, key) > 0) {
                LOG.debug("key '{}' not found in HSET", key);
            }

            return;
        }

        TaskWrapper<? extends Task> currentEnvelope = deserialize(Task.class, rawEnvelope);
        TaskWrapper<? extends Task> nextEnvelope = TaskWrapper.nextAttempt(currentEnvelope);

        // Start a transaction
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

        LOG.debug("notify task [{}]", currentEnvelope);
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

    /**
     * Serialize TaskWrapper to string
     *
     * @param envelope TaskWrapper
     * @return Serialized string
     */
    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    private String serialize(TaskWrapper<? extends Task> envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Deserialize string to TaskWrapper
     *
     * @param taskType Task class
     * @param rawEnvelope String to be deserialized
     * @param <T> Task type
     * @return TaskWrapper<T>
     */
    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    private <T extends Task> TaskWrapper<T> deserialize(Class<T> taskType, String rawEnvelope) {
        JavaType envelopeType = mapper.getTypeFactory().constructParametricType(TaskWrapper.class, taskType);
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

    private String toQueueName(Class<? extends Task> cls) {
        return dataSetPrefix + cls.getSimpleName().toLowerCase();
    }

    static String getKey(Task task) {
        return task.getClass().getName() + DELIMITER + task.getId();
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
