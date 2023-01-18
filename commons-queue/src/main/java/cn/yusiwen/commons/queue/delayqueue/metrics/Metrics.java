package cn.yusiwen.commons.queue.delayqueue.metrics;

import java.util.function.Supplier;

import cn.yusiwen.commons.queue.delayqueue.Event;

/**
 * @author Siwen Yu
 * @since 1.0.0
 */
public interface Metrics {

    <T extends Event> void incrementEnqueueCounter(Class<T> type);

    <T extends Event> void incrementDequeueCounter(Class<T> type);

    void registerScheduledCountSupplier(Supplier<Number> countSupplier);

    <T extends Event> void registerReadyToProcessSupplier(Class<T> type, Supplier<Number> countSupplier);
}
