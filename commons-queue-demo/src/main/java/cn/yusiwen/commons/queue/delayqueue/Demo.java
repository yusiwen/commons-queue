package cn.yusiwen.commons.queue.delayqueue;

import static java.lang.Boolean.TRUE;

import static cn.yusiwen.commons.queue.delayqueue.RedisDelayQueue.redisDelayQueue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.beans.ConstructorProperties;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.yusiwen.commons.queue.delayqueue.context.DefaultTaskContextHandler;
import cn.yusiwen.commons.queue.delayqueue.metrics.NoopMetrics;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.lettuce.core.RedisClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@SuppressFBWarnings("HES_EXECUTOR_NEVER_SHUTDOWN")
public class Demo {

    private static class DemoTask implements Task {

        /**
         * Id
         */
        @JsonProperty
        private final String id;

        /**
         * Name
         */
        private final String name;

        @ConstructorProperties({"id", "name"})
        private DemoTask(String id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DemoTask)) {
                return false;
            }
            DemoTask that = (DemoTask)o;
            return Objects.equals(id, that.id) && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id) * Objects.hash(name);
        }
    }

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(Demo.class);

    /**
     * Scheduling batch size
     */
    private static final int SCHEDULING_BATCH_SIZE = 50;
    /**
     * Polling timeout
     */
    private static final Duration POLLING_TIMEOUT = Duration.ofSeconds(1);

    /**
     * ObjectMapper
     */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Thread pool
     */
    private final ExecutorService executor =
        new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new ThreadFactory() {

            /**
             * Thread number
             */
            final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(@NotNull Runnable r) {
                return new Thread(r, "handler" + threadNumber.getAndIncrement());
            }
        });

    /**
     * RedisClient
     */
    private RedisClient redisClient;

    /**
     * RedisDelayQueue
     */
    private RedisDelayQueue queue;

    public void init() {
        redisClient = RedisClient.create("redis://192.168.3.1:6379");
        RedisDelayQueue.Builder builder = redisDelayQueue();
        builder.client(redisClient);
        builder.mapper(objectMapper);
        builder.handlerScheduler(Schedulers.fromExecutorService(executor));
        builder.enableScheduling(true);
        builder.schedulingInterval(Duration.ofSeconds(1));
        builder.schedulingBatchSize(SCHEDULING_BATCH_SIZE);
        builder.pollingTimeout(POLLING_TIMEOUT);
        builder.taskContextHandler(new DefaultTaskContextHandler());
        builder.dataSetPrefix("");
        builder.retryAttempts(10);
        builder.metrics(new NoopMetrics());
        builder.refreshSubscriptionsInterval(Duration.ofMinutes(5));
        queue = builder.build();
    }

    public void shutdown() {
        queue.close();
        redisClient.shutdown();
    }

    public void start() {
        queue.addTaskHandler(DemoTask.class, e -> Mono.fromCallable(() -> {
            LOG.info("DemoTask received, id = {}, name = {}", e.getId(), e.getName());
            return TRUE;
        }), 1);
        LOG.info("DemoEvent1 enqueue");
        queue.enqueue(new DemoTask("1", "DemoTask1"), Duration.ofSeconds(0)).subscribe();
        LOG.info("DemoEvent2 enqueue");
        queue.enqueue(new DemoTask("2", "DemoTask2"), Duration.ofSeconds(5)).subscribe();
        LOG.info("DemoEvent3 enqueue");
        queue.enqueue(new DemoTask("3", "DemoTask3"), Duration.ofSeconds(10)).subscribe();
        LOG.info("DemoEvent4 enqueue");
        queue.enqueue(new DemoTask("4", "DemoTask4"), Duration.ofSeconds(15)).subscribe();
        LOG.info("DemoEvent5 enqueue");
        queue.enqueue(new DemoTask("5", "DemoTask5"), Duration.ofSeconds(20)).subscribe();
    }

    public static void main(String[] args) {
        Demo demo = new Demo();
        Runtime.getRuntime().addShutdownHook(new Thread(demo::shutdown));
        demo.init();
        demo.start();
        LOG.info("main exited");
    }
}
