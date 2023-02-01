package cn.yusiwen.commons.queue.delayqueue;

import io.lettuce.core.dynamic.Commands;
import io.lettuce.core.dynamic.annotation.Command;
import io.lettuce.core.dynamic.batch.BatchExecutor;
import io.lettuce.core.dynamic.batch.BatchSize;

/**
 * Demo command for batch SADD
 *
 * @author Siwen Yu (yusiwen@gmail.com)
 */
@BatchSize(1000)
public interface DemoCommand extends Commands, BatchExecutor {
    @Command("SADD ?0 ?1")
    void add(String key, String msg);
}
