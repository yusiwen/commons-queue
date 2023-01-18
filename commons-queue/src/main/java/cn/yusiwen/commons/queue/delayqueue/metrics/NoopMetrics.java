package cn.yusiwen.commons.queue.delayqueue.metrics;

import java.util.function.Supplier;

import cn.yusiwen.commons.queue.delayqueue.Event;

/**
 * @author Siwen Yu
 * @since 1.0.0
 */
public class NoopMetrics implements Metrics {

    @Override
    public <T extends Event> void incrementEnqueueCounter(Class<T> type) {
        // no-op
    }

    @Override
    public <T extends Event> void incrementDequeueCounter(Class<T> type) {
        // no-op
    }

    @Override
    public void registerScheduledCountSupplier(Supplier<Number> countSupplier) {
        // no-op
    }

    @Override
    public <T extends Event> void registerReadyToProcessSupplier(Class<T> type, Supplier<Number> countSupplier) {
        // no-op
    }
}
