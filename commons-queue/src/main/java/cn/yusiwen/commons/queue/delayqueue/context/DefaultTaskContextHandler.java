package cn.yusiwen.commons.queue.delayqueue.context;

import static java.util.Collections.emptyMap;

import java.util.Map;

import reactor.util.context.Context;

/**
 * DefaultTaskContextHandler
 * <p>
 * Default context handler
 *
 * @author Siwen Yu
 * @since 1.0.0
 */
public final class DefaultTaskContextHandler implements TaskContextHandler {

    /**
     * Default key for context
     */
    private static final String KEY = "taskContext";

    @Override
    public Map<String, String> taskContext(Context subscriptionContext) {
        return subscriptionContext.getOrDefault(KEY, emptyMap());
    }

    @Override
    public Context subscriptionContext(Context originalSubscriptionContext, Map<String, String> taskContext) {
        return originalSubscriptionContext.put(KEY, taskContext);
    }
}
