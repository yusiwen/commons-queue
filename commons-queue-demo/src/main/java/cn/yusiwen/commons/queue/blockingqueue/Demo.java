package cn.yusiwen.commons.queue.blockingqueue;

import net.bytebuddy.agent.ByteBuddyAgent;

import java.lang.instrument.Instrumentation;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Demo {

    private final ReentrantLock acquireLock = new ReentrantLock();

    /**
     * Condition of not limited
     */
    private final Condition shutdown = acquireLock.newCondition();
    private boolean isShutdown = false;

    CountDownLatch latch = new CountDownLatch(3);

    void log(String msg) {
        System.out.println(Thread.currentThread() + ": " + msg);
    }

    void doInThread() {
        acquireLock.lock();
        try {
            log("alive");
            while (!isShutdown) {
                log("await");
                shutdown.await();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            acquireLock.unlock();
        }
        log("done");
        latch.countDown();
    }

    void doWork() throws InterruptedException {
        int maxFreeMemory = (int) MemoryLimitCalculator.maxAvailable();
        MemorySafeLinkedBlockingQueue<Runnable> queue = new MemorySafeLinkedBlockingQueue<>(maxFreeMemory);
        ExecutorService es = new ThreadPoolExecutor(2, 2, 500, TimeUnit.MILLISECONDS,
                queue,
                Executors.defaultThreadFactory(), (r, executor) -> {
            log("Task " + r.toString() +
                    " rejected from " +
                    executor.toString());
        });

        // Make core threads busy
        log("add 2 task to make core threads busy");
        for (int i = 0; i < 2; i++) {
            es.execute(this::doInThread);
        }

        // This task should be rejected.
        log("add one more task, this task should be rejected, because there is no more memory available");
        es.execute(this::doInThread);

        ByteBuddyAgent.install();
        final Instrumentation instrumentation = ByteBuddyAgent.getInstrumentation();
        final long objectSize = instrumentation.getObjectSize((Runnable) this::doInThread);
        // Set max available memory to make more available slot for one task
        log("set new max available memory");
        queue.setMaxFreeMemory((int)(MemoryLimitCalculator.maxAvailable() - objectSize));

        // This task should be queued.
        log("add one more task, this task should be queued");
        es.execute(this::doInThread);

        Thread.sleep(5000);

        // Wake up core threads
        acquireLock.lock();
        try {
            isShutdown = true;
            shutdown.signalAll();
        } finally {
            acquireLock.unlock();
        }

        latch.await();
        es.shutdownNow();
        MemoryLimitCalculator.shutdown();
    }

    public static void main(String[] args) {
        Demo demo = new Demo();
        try {
            demo.doWork();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
