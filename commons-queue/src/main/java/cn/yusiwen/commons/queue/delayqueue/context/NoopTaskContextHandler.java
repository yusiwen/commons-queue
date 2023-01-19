package cn.yusiwen.commons.queue.delayqueue.context;

import static java.util.Collections.emptyMap;

import java.util.Map;

import reactor.util.context.Context;

/**
 * NoopTaskContextHandler
 * <p>
 * Empty context handler
 *
 * @author Siwen Yu
 * @since 1.0.0
 */
public final class NoopTaskContextHandler implements TaskContextHandler {

    @Override
    public Map<String, String> taskContext(Context subscriptionContext) {
        return emptyMap();
    }

    @Override
    public Context subscriptionContext(Context originalSubscriptionContext, Map<String, String> taskContext) {
        return originalSubscriptionContext;
    }
}
