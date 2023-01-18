package cn.yusiwen.commons.queue.delayqueue.context;

import java.util.Map;

import reactor.util.context.Context;

/**
 * @author Siwen Yu
 * @since 1.0.0
 */
public interface EventContextHandler {

    /**
     * Extract an event context from a subscription context at the enqueue stage
     *
     * @param subscriptionContext Context
     * @return Map<String, String>
     */
    Map<String, String> eventContext(Context subscriptionContext);

    /**
     * Add an event context to a subscription context before passing an event to a handler
     *
     * @param eventContext Map<String, String>
     * @param originalSubscriptionContext Context
     * @return Context
     */
    Context subscriptionContext(Context originalSubscriptionContext, Map<String, String> eventContext);
}
