package cn.yusiwen.commons.queue.delayqueue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.beans.ConstructorProperties;
import java.util.Map;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Wrapper for Task
 * <p>
 * Make <code>Task</code> compatible with Redis
 *
 * @author Siwen Yu
 * @since 1.0.0
 * @param <T> Task
 */
@SuppressFBWarnings("JACKSON_UNSAFE_DESERIALIZATION")
final class TaskWrapper<T extends Task> {

    /**
     * Payload
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
    private final T payload;
    /**
     * Attempt times
     */
    @JsonProperty
    private final int attempt;
    /**
     * Log context
     */
    @JsonProperty
    private final Map<String, String> logContext;

    @ConstructorProperties({"payload", "attempt", "logContext"})
    private TaskWrapper(T payload, int attempt, Map<String, String> logContext) {
        this.payload = payload;
        this.attempt = attempt;
        this.logContext = logContext;
    }

    static <R extends Task> TaskWrapper<R> create(R payload, Map<String, String> logContext) {
        return new TaskWrapper<>(payload, 1, logContext);
    }

    static <R extends Task> TaskWrapper<R> nextAttempt(TaskWrapper<R> current) {
        return new TaskWrapper<>(current.payload, current.attempt + 1, current.logContext);
    }

    @SuppressWarnings("unchecked")
    Class<T> getType() {
        return (Class<T>)payload.getClass();
    }

    T getPayload() {
        return this.payload;
    }

    int getAttempt() {
        return this.attempt;
    }

    Map<String, String> getLogContext() {
        return this.logContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof TaskWrapper)) {
            return false;
        } else {
            TaskWrapper<?> that = (TaskWrapper<?>)o;
            return this.attempt == that.attempt && Objects.equals(this.payload, that.payload)
                && Objects.equals(this.logContext, that.logContext);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.payload, this.attempt, this.logContext);
    }

    @Override
    public String toString() {
        return String.format("redis task %s#%s with attempt %s", payload.getClass().getName(), payload.getId(),
            attempt);
    }
}
