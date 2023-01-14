package cn.yusiwen.commons.queue.rqueue;

import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.HOURS;

import static cn.yusiwen.commons.queue.rqueue.DelayedEventService.delayedEventService;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.beans.ConstructorProperties;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.yusiwen.commons.queue.rqueue.context.DefaultEventContextHandler;
import cn.yusiwen.commons.queue.rqueue.metrics.NoopMetrics;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.lettuce.core.RedisClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@SuppressFBWarnings("HES_EXECUTOR_NEVER_SHUTDOWN")
public class Demo {

    private static class DemoEvent implements Event {

        /**
         * Id
         */
        @JsonProperty
        private final String id;

        @ConstructorProperties({"id"})
        private DemoEvent(String id) {
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
            if (!(o instanceof DemoEvent)) {
                return false;
            }
            DemoEvent that = (DemoEvent)o;
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
     * Demo
     */
    private static Demo demo = new Demo();

    /**
     * ObjectMapper
     */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Thread pool
     */
    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    /**
     * RedisClient
     */
    private RedisClient redisClient;

    /**
     * DelayedEventService
     */
    private DelayedEventService eventService;

    public void init() {
        redisClient = RedisClient.create("redis://192.168.3.1:6379");
        eventService = delayedEventService().client(redisClient).mapper(objectMapper)
            .handlerScheduler(Schedulers.fromExecutorService(executor)).schedulingInterval(Duration.ofSeconds(1))
            .schedulingBatchSize(SCHEDULING_BATCH_SIZE).enableScheduling(false).pollingTimeout(POLLING_TIMEOUT)
            .eventContextHandler(new DefaultEventContextHandler()).dataSetPrefix("").retryAttempts(10)
            .metrics(new NoopMetrics()).refreshSubscriptionsInterval(Duration.ofMinutes(5)).build();
    }

    public void shutdown() {
        eventService.close();
        redisClient.shutdown();
    }

    public void start() {
        eventService.addHandler(DemoEvent.class, e -> Mono.fromCallable(() -> {
            LOG.info("DemoEvent received");
            return TRUE;
        }), 1);
        eventService.enqueue(new DemoEvent("1"), Duration.ofSeconds(10)).subscribe();
        eventService.dispatchDelayedMessages();
    }

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> demo.shutdown()));
        demo.init();
        demo.start();

        while (true) {
            try {
                HOURS.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
