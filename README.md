# Commons Queue

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://GitHub.com/yusiwen/commons-queue/graphs/commit-activity)
[![GitHub tag](https://img.shields.io/github/tag/yusiwen/commons-queue.svg)](https://GitHub.com/yusiwen/commons-queue/tags/)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/cn.yusiwen.commons/commons-queue/badge.svg)](https://maven-badges.herokuapp.com/maven-central/cn.yusiwen.commons/commons-queue)
[![Build Status](https://ci-github.yusiwen.cn/api/badges/yusiwen/commons-queue/status.svg)](https://ci-github.yusiwen.cn/yusiwen/commons-queue)

My common queue library implements queue operations. 

The purpose of the library is to provide higher-level abstractions of queue operations.

## Features

- DelayQueue implemented by [Redis](https://redis.io/) [sorted set](https://redis.io/docs/data-types/sorted-sets/)
- MemoryLimitedLinkedBlockingQueue: LinkedBlockingQueue with limited memory footprint
- MemorySafeLinkedBlockingQueue: Memory safe LinkedBlockingQueue

## Usages

```xml
<dependency>
    <groupId>cn.yusiwen.commons</groupId>
    <artifactId>commons-queue</artifactId>
    <version>1.0.2.1</version>
</dependency>
```

### RedisDelayQueue

Initialize queue:

```java
public void init() {
    redisClient = RedisClient.create("redis://<redis-server>:6379");
    RedisDelayQueue.Builder builder = redisDelayQueue();
    builder.client(redisClient); // Set redis client
    builder.mapper(objectMapper); // Set ObjectMapper
    builder.handlerScheduler(Schedulers.fromExecutorService(executor)); // Set scheduler for handlers
    builder.enableScheduling(true); // Enable scheduling
    builder.schedulingInterval(Duration.ofSeconds(1)); // Set scheduling interval
    builder.schedulingBatchSize(SCHEDULING_BATCH_SIZE); // Set prefetch size for backpressure
    builder.pollingTimeout(POLLING_TIMEOUT); // Set polling timeout
    builder.taskContextHandler(new DefaultTaskContextHandler());
    builder.dataSetPrefix("");
    builder.retryAttempts(10); // Set max attempts
    builder.metrics(new NoopMetrics());
    builder.refreshSubscriptionsInterval(Duration.ofMinutes(5)); // Set interval for refreshing subcription 
    queue = builder.build();
}
```

Add handler and task:

```java
queue.addTaskHandler(DemoTask.class, e -> Mono.fromCallable(() -> {
    LOG.info("DemoTask received, id = {}", e.getId());
    return TRUE;
}), 1);

queue.enqueue(new DemoTask("1"), Duration.ofSeconds(10)).subscribe();
```

The `DemoTask` will be triggered in 10 seconds.

More details in [Demo.java](https://github.com/yusiwen/commons-queue/blob/master/commons-queue-demo/src/main/java/cn/yusiwen/commons/queue/delayqueue/Demo.java)

### MemoryLimitedLinkedBlockingQueue and MemorySafeLinkedBlockingQueue

```java
MemorySafeLinkedBlockingQueue<MyData> queue = new MemorySafeLinkedBlockingQueue<>(maxFreeMemory);

MemoryLimitedLinkedBlockingQueue<MyData> queue = new MemoryLimitedLinkedBlockingQueue<>(memoryLimit, instrumentation)
```

See [Demo.java](https://github.com/yusiwen/commons-queue/blob/master/commons-queue-demo/src/main/java/cn/yusiwen/commons/queue/blockingqueue/Demo.java) for more details.