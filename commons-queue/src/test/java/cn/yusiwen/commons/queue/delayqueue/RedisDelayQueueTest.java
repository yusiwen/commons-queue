package cn.yusiwen.commons.queue.delayqueue;

import static java.util.Collections.singletonMap;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

import static cn.yusiwen.commons.queue.delayqueue.RedisDelayQueue.redisDelayQueue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import cn.yusiwen.commons.queue.delayqueue.context.DefaultTaskContextHandler;
import cn.yusiwen.commons.queue.delayqueue.metrics.NoopMetrics;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.sync.RedisCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

class RedisDelayQueueTest {

    private static class DummyTask implements Task {

        @JsonProperty
        private final String id;

        @ConstructorProperties({"id"})
        private DummyTask(String id) {
            this.id = id;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DummyTask)) {
                return false;
            }
            DummyTask that = (DummyTask)o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private static class DummyTask2 implements Task {

        @JsonProperty
        private final String id;

        @ConstructorProperties({"id"})
        private DummyTask2(String id) {
            this.id = id;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DummyTask2)) {
                return false;
            }
            DummyTask2 that = (DummyTask2)o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private static class DummyTask3 implements Task {

        @JsonProperty
        private final String id;

        @ConstructorProperties({"id"})
        private DummyTask3(String id) {
            this.id = id;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DummyTask3)) {
                return false;
            }
            DummyTask3 that = (DummyTask3)o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private static final String DELAYED_QUEUE = "delayed_tasks";
    private static final String TOXIPROXY_IP = ofNullable(System.getenv("TOXIPROXY_IP")).orElse("127.0.0.1");

    private static final Duration POLLING_TIMEOUT = Duration.ofSeconds(1);
    private static final Function<DummyTask, Mono<Boolean>> DUMMY_HANDLER = e -> Mono.just(true);
    private static final int SCHEDULING_BATCH_SIZE = 50;

    private RedisClient redisClient;
    private RedisCommands<String, String> connection;
    private RedisDelayQueue eventService;
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ToxiproxyClient toxiProxyClient = new ToxiproxyClient(TOXIPROXY_IP, 8474);
    private Proxy redisProxy;

    @BeforeEach
    void setUp() throws IOException {
        removeOldProxies();
        redisProxy = createRedisProxy();
        redisClient = RedisClient.create("redis://" + TOXIPROXY_IP + ":63790");
        redisClient.setOptions(ClientOptions.builder()
            .timeoutOptions(TimeoutOptions.builder().timeoutCommands().fixedTimeout(Duration.ofMillis(500)).build())
            .build());

        eventService = redisDelayQueue().client(redisClient).mapper(objectMapper)
            .handlerScheduler(Schedulers.fromExecutorService(executor)).schedulingInterval(Duration.ofSeconds(1))
            .schedulingBatchSize(SCHEDULING_BATCH_SIZE).enableScheduling(false).pollingTimeout(POLLING_TIMEOUT)
            .taskContextHandler(new DefaultTaskContextHandler()).dataSetPrefix("").retryAttempts(10)
            .metrics(new NoopMetrics()).refreshSubscriptionsInterval(Duration.ofMinutes(5)).build();

        connection = redisClient.connect().sync();
        connection.flushall();

        MDC.clear();
    }

    @AfterEach
    void releaseConnection() {
        eventService.close();
        redisClient.shutdown();
    }

    @Test
    void shouldHandleDifferentEventsInParallel() {
        // given
        int total = 30;
        CountDownLatch latch = new CountDownLatch(total);

        eventService.addTaskHandler(DummyTask.class, e -> Mono.fromCallable(() -> randomSleepBeforeCountdown(latch)),
            3);
        eventService.addTaskHandler(DummyTask2.class, e -> Mono.fromCallable(() -> randomSleepBeforeCountdown(latch)),
            3);
        eventService.addTaskHandler(DummyTask3.class, e -> Mono.fromCallable(() -> randomSleepBeforeCountdown(latch)),
            3);
        // and events are queued
        enqueue(total, id -> {
            String str = Integer.toString(id);
            switch (id % 3) {
                case 2:
                    return new DummyTask3(str);
                case 1:
                    return new DummyTask2(str);
                default:
                    return new DummyTask(str);
            }
        }).block();

        assertEventsCount(total);
        // when
        eventService.dispatchDelayedTasks();
        // then
        waitAndAssertEventsCount(0L);
    }

    @Test
    void shouldAdherePrefetchLimit() {
        // given
        int total = 10;
        int prefetch = 1;
        Semaphore sem = new Semaphore(1);

        eventService.addTaskHandler(DummyTask.class, e -> Mono.fromCallable(() -> {
            sem.acquireUninterruptibly();
            return true;
        }), prefetch);
        // and events are queued
        enqueue(total).block();
        assertEventsCount(total);
        // when
        eventService.dispatchDelayedTasks();
        // then
        waitAndAssertEventsCount(total - prefetch);
    }

    @Test
    void shouldProvideContextToHandler() {
        // given
        String contextValue = "context";
        AtomicReference<String> holder = new AtomicReference<>();
        eventService.addTaskHandler(DummyTask.class, e -> Mono.subscriberContext().doOnNext(ctx -> {
            Map<String, String> eventContext = ctx.get("eventContext");
            holder.set(eventContext.get("key"));
        }).thenReturn(true), 1);
        // and events are queued with context
        enqueue(1).subscriberContext(ctx -> ctx.put("eventContext", singletonMap("key", contextValue))).block();
        // when
        eventService.dispatchDelayedTasks();
        // then
        waitAndAssertEventsCount(0);
        assertThat(holder.get(), equalTo(contextValue));
    }

    @Test
    void shouldCompleteInTimelyMannerForLongRunningHandlers() throws InterruptedException {
        // given
        final int total = 20;
        final int timeout = 500;
        final int prefetch = 10;
        CountDownLatch latch = new CountDownLatch(20);

        eventService.addTaskHandler(DummyTask.class, e -> Mono.fromCallable(() -> {
            try {
                MILLISECONDS.sleep(500);
                latch.countDown();
                return true;
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }), prefetch);
        // and events are enqueued
        enqueue(total).block();
        assertEventsCount(total);
        // when
        eventService.dispatchDelayedTasks();
        // then task execution takes 500, 20 should complete in 1000 (with prefetch of 10), 100 ms as reserve
        assertThat(latch.await(total / prefetch * timeout + 100, MILLISECONDS), equalTo(true));
    }

    @Test
    void shouldBeAbleToEnqueueConcurrently() {
        // when
        Flux.merge(IntStream.range(0, 50).parallel()
            .mapToObj(id -> eventService.enqueue(new DummyTask(Integer.toString(id)), Duration.ZERO)).collect(toList()))
            .then().block();
        // then
        assertEventsCount(50L);
    }

    @Test
    void shouldHandleDeserializationError() throws InterruptedException {
        // given
        int total = 20;
        // and events are enqueued
        enqueue(total).block();
        waitAndAssertEventsCount(total);
        // and a malformed event is placed in the beginning of a list
        connection.lpush(toQueueName(DummyTask.class), "[unserializable}");
        eventService.dispatchDelayedTasks();
        // and all events are moved to list
        assertThat(connection.llen(toQueueName(DummyTask.class)), equalTo((long)total + 1));
        // when
        CountDownLatch latch = new CountDownLatch(total + 1);
        eventService.addTaskHandler(DummyTask.class, e -> Mono.fromCallable(() -> {
            latch.countDown();
            return true;
        }), 1);
        // then
        latch.await(5000, MILLISECONDS);
        waitAndAssertEventsCount(0L);
    }

    @Test
    void shouldBeAbleToReconnect() throws InterruptedException, IOException {
        // given
        eventService.addTaskHandler(DummyTask.class, DUMMY_HANDLER, 1);
        // when connection is broken
        redisProxy.delete();
        MILLISECONDS.sleep(POLLING_TIMEOUT.toMillis() + 100);
        // and connection is restored
        redisProxy = createRedisProxy();
        // then new event is handled
        enqueue(1).block();
        assertEventsCount(1L);
        eventService.dispatchDelayedTasks();
        waitAndAssertEventsCount(0L);
    }

    @Test
    void shouldHandlePollingTimeout() throws InterruptedException {
        // given
        eventService.addTaskHandler(DummyTask.class, DUMMY_HANDLER, 1);
        // and no events have arrived during a polling interval
        MILLISECONDS.sleep(POLLING_TIMEOUT.toMillis() + 100);
        // and event is queued
        enqueue(1).block();
        assertEventsCount(1L);
        // then
        eventService.dispatchDelayedTasks();
        waitAndAssertEventsCount(0L);
    }

    @Test
    void shouldFailToSubscribeIfConnectionNotAvailable() throws IOException {
        redisProxy.delete();

        assertThrows(RedisConnectionException.class,
            () -> eventService.addTaskHandler(DummyTask.class, DUMMY_HANDLER, 1));
    }

    @Test
    void shouldFailToDispatchIfConnectionNotAvailable() throws IOException {
        redisProxy.delete();

        assertThrows(RedisCommandTimeoutException.class, () -> eventService.dispatchDelayedTasks());
    }

    @Test
    void shouldHandleSubscriberErrors() {
        // given an erroneous handler
        int total = 6;
        CountDownLatch latch = new CountDownLatch(total);

        eventService.addTaskHandler(DummyTask.class, e -> {
            latch.countDown();

            switch ((Integer.parseInt(e.getId())) % total) {
                // valid
                case 0:
                    return Mono.just(true);
                // invalid
                case 1:
                    return Mono.error(new RuntimeException("no-no"));
                case 2:
                    return Mono.empty();
                case 3:
                    return null;
                case 4:
                    throw new RuntimeException("oops");
                default:
                    return Mono.just(false);
            }
        }, 1);
        // and events are enqueued
        enqueue(total).block();
        waitAndAssertEventsCount(total);
        // when
        eventService.dispatchDelayedTasks();
        // then only valid events are handled
        waitAndAssertEventsCount(total - 1);
    }

    @Test
    void shouldRescheduleEventForLaterTimeDuringDispatch() {
        // given event is queued
        DummyTask event = new DummyTask("99");
        eventService.enqueue(event, Duration.ZERO).block();

        double initialScore = connection.zscore(DELAYED_QUEUE, RedisDelayQueue.getKey(event));
        // when
        eventService.dispatchDelayedTasks();
        double postDispatchScore = connection.zscore(DELAYED_QUEUE, RedisDelayQueue.getKey(event));
        // then the post dispatch score is 10 sec ahead
        assertThat(postDispatchScore - initialScore, greaterThan(10000.0));
    }

    @Test
    void shouldAdhereDispatchLimit() {
        // given
        int extra = 20;
        int total = SCHEDULING_BATCH_SIZE + extra;
        // and events are scheduled
        enqueue(total).block();
        assertEventsCount(total);
        long maxScore = System.currentTimeMillis();
        // when
        eventService.dispatchDelayedTasks();
        // then only SCHEDULING_BATCH_SIZE are rescheduled
        assertThat(connection.zcount(DELAYED_QUEUE, Range.create(0, maxScore)), equalTo((long)extra));
    }

    @Test
    void shouldNotDuplicateSameEvents() {
        // given
        DummyTask event = new DummyTask("1");
        // when
        eventService.enqueue(event, Duration.ZERO).block();
        eventService.enqueue(event, Duration.ZERO).block();
        // then
        assertEventsCount(1L);
    }

    @Test
    void shouldBeAbleToremoveTaskHandler() {
        // given
        eventService.addTaskHandler(DummyTask.class, DUMMY_HANDLER, 1);
        // and events are enqueued
        enqueue(10).block();
        eventService.dispatchDelayedTasks();
        waitAndAssertEventsCount(0L);
        // when
        assertThat(eventService.removeTaskHandler(DummyTask.class), equalTo(true));
        assertThat(eventService.removeTaskHandler(DummyTask.class), equalTo(false));
        // then new events are not handled
        enqueue(10).block();
        waitAndAssertEventsCount(10L);
    }

    @Test
    void shouldReleaseOldConnectionOnSubscriptionRefresh() {
        // given
        int initNumber = serviceConnectionsCount();
        // when
        eventService.addTaskHandler(DummyTask.class, DUMMY_HANDLER, 1);
        sleepMillis(100);
        assertThat(serviceConnectionsCount() - initNumber, is(1));
        // and subscription is refreshed
        eventService.refreshSubscriptions();
        sleepMillis(100);
        // then
        assertThat(serviceConnectionsCount() - initNumber, is(1));
    }

    @Test
    void shouldNotAllowToEnqueueNulls() {
        assertThrows(NullPointerException.class, () -> eventService.enqueue(new DummyTask("1"), null));
        assertThrows(NullPointerException.class, () -> eventService.enqueue(null, Duration.ZERO));
        assertThrows(NullPointerException.class, () -> eventService.enqueue(new DummyTask(null), Duration.ZERO));
    }

    @Test
    void shouldValidateAddedHandler() {
        assertThrows(NullPointerException.class, () -> eventService.addTaskHandler(null, DUMMY_HANDLER, 1));
        assertThrows(NullPointerException.class, () -> eventService.addTaskHandler(DummyTask.class, null, 1));
        assertThrows(IllegalArgumentException.class,
            () -> eventService.addTaskHandler(DummyTask.class, DUMMY_HANDLER, 0));
        assertThrows(IllegalArgumentException.class,
            () -> eventService.addTaskHandler(DummyTask.class, DUMMY_HANDLER, 101));
    }

    private void removeOldProxies() throws IOException {
        toxiProxyClient.getProxies().forEach(p -> {
            try {
                p.delete();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private Proxy createRedisProxy() throws IOException {
        return toxiProxyClient.createProxy("redis", TOXIPROXY_IP + ":63790", "192.168.3.1:6379");
    }

    private Mono<Void> enqueue(int num) {
        return enqueue(IntStream.range(0, num));
    }

    private Mono<Void> enqueue(int num, Function<Integer, Task> transformer) {
        return enqueue(IntStream.range(0, num), transformer);
    }

    private Mono<Void> enqueue(IntStream stream) {
        return enqueue(stream, id -> new DummyTask(Integer.toString(id)));
    }

    private Mono<Void> enqueue(IntStream stream, Function<Integer, Task> transformer) {
        return Flux.fromStream(stream::boxed).flatMap(id -> eventService.enqueue(transformer.apply(id), Duration.ZERO))
            .then();
    }

    private String toQueueName(Class<? extends Task> cls) {
        return cls.getSimpleName().toLowerCase();
    }

    private boolean randomSleepBeforeCountdown(CountDownLatch latch) {
        sleepMillis(1 + new Random().nextInt(30));
        latch.countDown();
        return true;
    }

    private void waitAndAssertEventsCount(long expected) {
        for (int i = 0; i < 5; i++) {
            sleepMillis(25);

            if (connection.zcard(DELAYED_QUEUE) == expected) {
                return;
            }
        }

        assertEventsCount(expected);
    }

    private void assertEventsCount(long expected) {
        assertThat(connection.zcard(DELAYED_QUEUE), equalTo(expected));
    }

    private void sleepMillis(long duration) {
        try {
            MILLISECONDS.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private int serviceConnectionsCount() {
        return connection.clientList().split("\\r?\\n").length;
    }
}
