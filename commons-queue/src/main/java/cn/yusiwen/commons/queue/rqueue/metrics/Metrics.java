package cn.yusiwen.commons.queue.rqueue.metrics;

import java.util.function.Supplier;

import cn.yusiwen.commons.queue.rqueue.Event;

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
