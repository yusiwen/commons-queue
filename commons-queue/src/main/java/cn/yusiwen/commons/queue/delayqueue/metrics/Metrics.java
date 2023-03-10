package cn.yusiwen.commons.queue.delayqueue.metrics;

import java.util.function.Supplier;

import cn.yusiwen.commons.queue.delayqueue.Task;

/**
 * @author Siwen Yu
 * @since 1.0.0
 */
public interface Metrics {

    <T extends Task> void incrementEnqueueCounter(Class<T> type);

    <T extends Task> void incrementDequeueCounter(Class<T> type);

    void registerScheduledCountSupplier(Supplier<Number> countSupplier);

    <T extends Task> void registerReadyToProcessSupplier(Class<T> type, Supplier<Number> countSupplier);
}
