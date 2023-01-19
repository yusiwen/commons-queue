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

        @ConstructorProperties({"id"})
        private DemoTask(String id) {
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
            if (!(o instanceof DemoTask)) {
                return false;
            }
            DemoTask that = (DemoTask)o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
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
    private RedisDelayQueue eventService;

    public void init() {
        redisClient = RedisClient.create("redis://192.168.3.1:6379");
        eventService = redisDelayQueue().client(redisClient).mapper(objectMapper)
            .handlerScheduler(Schedulers.fromExecutorService(executor)).enableScheduling(true)
            .schedulingInterval(Duration.ofSeconds(1)).schedulingBatchSize(SCHEDULING_BATCH_SIZE)
            .pollingTimeout(POLLING_TIMEOUT).taskContextHandler(new DefaultTaskContextHandler()).dataSetPrefix("")
            .retryAttempts(10).metrics(new NoopMetrics()).refreshSubscriptionsInterval(Duration.ofMinutes(5)).build();
    }

    public void shutdown() {
        eventService.close();
        redisClient.shutdown();
    }

    public void start() {
        eventService.addTaskHandler(DemoTask.class, e -> Mono.fromCallable(() -> {
            LOG.info("DemoTask received, id = {}", e.getId());
            return TRUE;
        }), 1);
        LOG.info("DemoEvent1 enqueue");
        eventService.enqueue(new DemoTask("1"), Duration.ofSeconds(10)).subscribe();
        LOG.info("DemoEvent2 enqueue");
        eventService.enqueue(new DemoTask("2"), Duration.ofSeconds(10)).subscribe();
        LOG.info("DemoEvent3 enqueue");
        eventService.enqueue(new DemoTask("3"), Duration.ofSeconds(10)).subscribe();
    }

    public static void main(String[] args) {
        Demo demo = new Demo();
        Runtime.getRuntime().addShutdownHook(new Thread(demo::shutdown));
        demo.init();
        demo.start();
        LOG.info("main exited");
    }
}
