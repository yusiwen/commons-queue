package cn.yusiwen.commons.queue.delayqueue;

import static java.lang.Boolean.TRUE;

import java.util.function.Function;

import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.yusiwen.commons.queue.delayqueue.context.TaskContextHandler;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * Helper class to handle the subscription of
 *
 * @author Siwen Yu
 * @since 1.0.0
 * @param <T> Task
 */
class InnerSubscriber<T extends Task> extends BaseSubscriber<TaskWrapper<T>> {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(InnerSubscriber.class);

    /**
     * TaskContextHandler
     */
    private final TaskContextHandler contextHandler;
    /**
     * Handler
     */
    private final Function<T, Mono<Boolean>> handler;
    /**
     * Parallelism
     */
    private final int parallelism;
    /**
     * StatefulRedisConnection for polling
     */
    private final StatefulRedisConnection<String, String> pollingConnection;
    /**
     * Scheduler
     */
    private final Scheduler handlerScheduler;
    /**
     * Delete command
     */
    private final Function<T, Mono<TransactionResult>> deleteCommand;

    InnerSubscriber(TaskContextHandler contextHandler, Function<T, Mono<Boolean>> handler, int parallelism,
        StatefulRedisConnection<String, String> pollingConnection, Scheduler handlerScheduler,
        Function<T, Mono<TransactionResult>> deleteCommand) {
        this.contextHandler = contextHandler;
        this.handler = handler;
        this.parallelism = parallelism;
        this.pollingConnection = pollingConnection;
        this.handlerScheduler = handlerScheduler;
        this.deleteCommand = deleteCommand;
    }

    @Override
    protected void hookOnSubscribe(@NotNull Subscription subscription) {
        requestInner(parallelism);
    }

    @Override
    protected void hookOnNext(@NotNull TaskWrapper<T> envelope) {
        LOG.debug("task [{}] received from queue", envelope);

        Mono<Boolean> promise;

        try {
            promise = handler.apply(envelope.getPayload());
        } catch (Exception e) {
            LOG.info("error in non-blocking handler for [{}]", envelope.getType(), e);
            requestInner(1);
            return;
        }

        if (promise == null) {
            requestInner(1);
            return;
        }

        promise.defaultIfEmpty(Boolean.FALSE)
            .doOnError(e -> LOG.warn("error occurred during handling task [{}]", envelope, e))
            .onErrorReturn(Boolean.FALSE).flatMap(completed -> {
                if (TRUE.equals(completed)) {
                    LOG.debug("deleting task {} from delayed queue", envelope.getPayload());
                    // todo we could also fail here!!! test me! with latch and toxyproxy
                    return deleteCommand.apply(envelope.getPayload()).map(r -> TRUE);
                } else {
                    return Mono.just(TRUE);
                }
            }).subscribeOn(handlerScheduler)
            .subscriberContext(c -> contextHandler.subscriptionContext(c, envelope.getLogContext())).subscribe(r -> {
                LOG.debug("task processing completed [{}]", envelope);
                requestInner(1);
            });
    }

    @Override
    protected void hookOnCancel() {
        pollingConnection.close();
        LOG.debug("subscription connection shut down");
    }

    private void requestInner(long n) {
        LOG.debug("requesting next {} elements", n);
        request(n);
    }

    @Override
    public void dispose() {
        pollingConnection.close();
        super.dispose();
    }
}
