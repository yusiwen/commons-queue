package cn.yusiwen.commons.queue.delayqueue.context;

import java.util.Map;

import reactor.util.context.Context;

/**
 * TaskContextHandler
 * <p>
 * Handling task context and subscriptions
 *
 * @author Siwen Yu
 * @since 1.0.0
 */
public interface TaskContextHandler {

    /**
     * Extract an task context from a subscription context at the enqueue stage
     *
     * @param subscriptionContext Context
     * @return {@code Map<String, String>}
     */
    Map<String, String> taskContext(Context subscriptionContext);

    /**
     * Add an task context to a subscription context before passing an event to a handler
     *
     * @param taskContext {@code Map<String, String>}
     * @param originalSubscriptionContext Context
     * @return Context
     */
    Context subscriptionContext(Context originalSubscriptionContext, Map<String, String> taskContext);
}
