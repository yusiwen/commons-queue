package cn.yusiwen.commons.queue.rqueue.context;

import static java.util.Collections.emptyMap;

import java.util.Map;

import reactor.util.context.Context;

/**
 * @author Siwen Yu
 * @since 1.0.0
 */
public final class DefaultEventContextHandler implements EventContextHandler {

    /**
     * Key
     */
    private static final String KEY = "eventContext";

    @Override
    public Map<String, String> eventContext(Context subscriptionContext) {
        return subscriptionContext.getOrDefault(KEY, emptyMap());
    }

    @Override
    public Context subscriptionContext(Context originalSubscriptionContext, Map<String, String> eventContext) {
        return originalSubscriptionContext.put(KEY, eventContext);
    }
}
