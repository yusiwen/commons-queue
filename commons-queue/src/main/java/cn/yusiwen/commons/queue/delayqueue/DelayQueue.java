package cn.yusiwen.commons.queue.delayqueue;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;

import reactor.core.publisher.Mono;

/**
 * DelayQueue
 * <p>
 * Interface of delay queue
 *
 * @author Siwen Yu
 * @since 1.0.0
 */
public interface DelayQueue {

    /**
     * Add task handler
     * <p>
     * A task handler is a {@code Function} receives a {@code Task} and returns a {@code Mono<Boolean>} which indicates
     * the handler execution status
     *
     * @param taskType Task class
     * @param handler Task handler
     * @param prefetch prefetch size
     * @param <T> Task type
     */
    <T extends Task> void addTaskHandler(@NotNull Class<T> taskType,
        @NotNull Function<@NotNull T, @NotNull Mono<Boolean>> handler, int prefetch);

    /**
     * Remove task handler of certain task type
     *
     * @param taskType Task class
     * @param <T> Task type
     * @return Successful removing for not
     */
    <T extends Task> boolean removeTaskHandler(@NotNull Class<T> taskType);

    /**
     * Add task into delay queue
     *
     * @param task Task
     * @param delay Delay duration
     * @param <T> Task type
     * @return A {@code Mono<Void>} which only replays complete and error signals
     */
    <T extends Task> Mono<Void> enqueue(@NotNull T task, @NotNull Duration delay);

    /**
     * Add task into delay queue with a given trigger time
     *
     * @param task Task
     * @param triggerTime Date of trigger
     * @param <T> Task type
     * @return A {@code Mono<Void>} which only replays complete and error signals
     */
    <T extends Task> Mono<Void> enqueue(@NotNull T task, @NotNull LocalDateTime triggerTime);

    /**
     * Add task into delay queue with a given crontab of trigger
     *
     * @param task Task
     * @param triggerCron Crontab of trigger
     * @param <T> Task type
     * @return A {@code Mono<Void>} which only replays complete and error signals
     */
    <T extends Task> Mono<Void> enqueue(@NotNull T task, @NotNull String triggerCron);
}
