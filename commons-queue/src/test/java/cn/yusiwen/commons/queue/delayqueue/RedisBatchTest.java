package cn.yusiwen.commons.queue.delayqueue;

import static java.util.Optional.ofNullable;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.dynamic.RedisCommandFactory;

/**
 * Test for Redis batch execution of set commands
 *
 * @author Siwen Yu (yusiwen@gmail.com)
 */
public class RedisBatchTest {

    private static final String TOXIPROXY_IP = ofNullable(System.getenv("TOXIPROXY_IP")).orElse("127.0.0.1");
    private RedisClient redisClient;
    private RedisCommands<String, String> connection;
    private final ToxiproxyClient toxiProxyClient = new ToxiproxyClient(TOXIPROXY_IP, 8474);
    private Proxy redisProxy;

    @BeforeEach
    void setUp() throws IOException {
        removeOldProxies();
        redisProxy = createRedisProxy();
        redisClient = RedisClient.create("redis://" + TOXIPROXY_IP + ":63790");
        redisClient.setOptions(ClientOptions.builder()
            .timeoutOptions(TimeoutOptions.builder().timeoutCommands().fixedTimeout(Duration.ofMillis(2000)).build())
            .build());

        prepareSet1();
        prepareSet2();

        MDC.clear();
    }

    @AfterEach
    void releaseConnection() {
        redisClient.connect().sync().del("demoSet1", "demoSet2");
        redisClient.shutdown();
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
        return toxiProxyClient.createProxy("redis", TOXIPROXY_IP + ":63790", "192.168.3.2:6379");
    }

    private void prepareSet1() {
        RedisCommandFactory factory = new RedisCommandFactory(redisClient.connect());

        List<RedisFuture<?>> futures = new ArrayList<>();
        DemoCommand demoCommand = factory.getCommands(DemoCommand.class);
        int count = 0;
        for (int i = 0; i < 10000; i++) {
            demoCommand.add("demoSet1", String.valueOf(i));
            count++;
        }
        if (count % 1000 != 0) {
            demoCommand.flush();
        }
    }

    private void prepareSet2() {
        RedisCommandFactory factory = new RedisCommandFactory(redisClient.connect());

        List<RedisFuture<?>> futures = new ArrayList<>();
        DemoCommand demoCommand = factory.getCommands(DemoCommand.class);
        int count = 0;
        for (int i = 5; i < 10005; i++) {
            demoCommand.add("demoSet2", String.valueOf(i));
            count++;
        }

        if (count % 1000 != 0) {
            demoCommand.flush();
        }
    }

    @Test
    void testSdiff() throws ExecutionException, InterruptedException {
        RedisAsyncCommands<String, String> commands = redisClient.connect().async();
        Set<String> actual = commands.sdiff("demoSet1", "demoSet2").get();
        Set<String> expected = new LinkedHashSet<>(Arrays.asList("0", "1", "2", "3", "4"));
        assertTrue(actual.containsAll(expected));
    }

    @Test
    void testSinter() throws ExecutionException, InterruptedException {
        RedisAsyncCommands<String, String> commands = redisClient.connect().async();
        Set<String> actual = commands.sinter("demoSet1", "demoSet2").get();
        Set<String> expected = new LinkedHashSet<>();
        for (int i = 5; i < 10000; i++) {
            expected.add(String.valueOf(i));
        }
        assertTrue(actual.containsAll(expected));
    }

}
