package cn.yusiwen.commons.queue.blockingqueque;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.lang.instrument.Instrumentation;

import net.bytebuddy.agent.ByteBuddyAgent;

import org.junit.jupiter.api.Test;

import cn.yusiwen.commons.queue.blockingqueue.MemoryLimitCalculator;
import cn.yusiwen.commons.queue.blockingqueue.MemorySafeLinkedBlockingQueue;

public class MemorySafeLinkedBlockingQueueTest {

    @Test
    public void test() throws Exception {
        ByteBuddyAgent.install();
        final Instrumentation instrumentation = ByteBuddyAgent.getInstrumentation();
        final long objectSize = instrumentation.getObjectSize((Runnable)() -> {
        });
        int maxFreeMemory = (int)MemoryLimitCalculator.maxAvailable();
        MemorySafeLinkedBlockingQueue<Runnable> queue = new MemorySafeLinkedBlockingQueue<>(maxFreeMemory);
        // all memory is reserved for JVM, so it will fail here
        assertThat(queue.offer(() -> {
        }), is(false));

        // maxFreeMemory-objectSize Byte memory is reserved for the JVM, so this will succeed
        queue.setMaxFreeMemory((int)(MemoryLimitCalculator.maxAvailable() - objectSize));
        assertThat(queue.offer(() -> {
        }), is(true));
    }
}
