/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.processor;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.subscription.RetryPolicy;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroup;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.common.utils.ExceptionUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiptHandleProcessor extends AbstractStartAndShutdown {
    // 定义一个日志记录器，用于记录当前类（一般是 ReceiptHandleProcessor 或其子类）的运行日志。
    // LoggerName.PROXY_LOGGER_NAME 通常是一个常量字符串，用来标识该模块的日志分类。
    // 通过 SLF4J 的 LoggerFactory 获取对应的 Logger 实例。
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    // 用于存储消息句柄组的并发安全 Map。
    // key：ReceiptHandleGroupKey（一般由 messageGroup、topic、consumerGroup 等信息组成）
    // value：ReceiptHandleGroup，表示某一组消息句柄的集合。
    // 每个组负责管理同一类消息的句柄（ReceiptHandle）的更新与可见性续期。
    protected final ConcurrentMap<ReceiptHandleGroupKey, ReceiptHandleGroup> receiptHandleGroupMap;

    // 定义一个单线程的定时任务线程池，用于周期性执行“消息句柄续期”任务。
    // “续期”指的是在消息可见性超时前，自动延长消息的可见时间，防止消息被其他消费者重复消费。
    // 线程命名格式为 “RenewalScheduledThread_”。
    protected final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RenewalScheduledThread_"));

    // 定义一个线程池，用于并行执行句柄续期（renewal）任务。
    // 当需要为多个消息同时续期时，使用该线程池进行任务分发与并发处理。
    protected ThreadPoolExecutor renewalWorkerService;

    // 消息处理器核心组件，用于与 Broker 交互（发送 ack、修改可见性、拉取消息等）。
    // ReceiptHandleProcessor 通过它执行实际的消息确认、续期请求等操作。
    protected final MessagingProcessor messagingProcessor;

    public ReceiptHandleProcessor(MessagingProcessor messagingProcessor) {
        this.messagingProcessor = messagingProcessor;
        this.receiptHandleGroupMap = new ConcurrentHashMap<>();
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        this.renewalWorkerService = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getRenewThreadPoolNums(),
            proxyConfig.getRenewMaxThreadPoolNums(),
            1, TimeUnit.MINUTES,
            "RenewalWorkerThread",
            proxyConfig.getRenewThreadPoolQueueCapacity()
        );
        this.init();
    }

    protected void init() {
        this.registerConsumerListener();
        this.renewalWorkerService.setRejectedExecutionHandler((r, executor) -> log.warn("add renew task failed. queueSize:{}", executor.getQueue().size()));
        this.appendStartAndShutdown(new StartAndShutdown() {
            @Override
            public void start() throws Exception {
                scheduledExecutorService.scheduleWithFixedDelay(() -> scheduleRenewTask(), 0,
                    ConfigurationManager.getProxyConfig().getRenewSchedulePeriodMillis(), TimeUnit.MILLISECONDS);
            }

            @Override
            public void shutdown() throws Exception {
                scheduledExecutorService.shutdown();
                clearAllHandle();
            }
        });
    }

    protected void registerConsumerListener() {
        this.messagingProcessor.registerConsumerListener(new ConsumerIdsChangeListener() {
            @Override
            public void handle(ConsumerGroupEvent event, String group, Object... args) {
                if (ConsumerGroupEvent.CLIENT_UNREGISTER.equals(event)) {
                    if (args == null || args.length < 1) {
                        return;
                    }
                    if (args[0] instanceof ClientChannelInfo) {
                        ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
                        clearGroup(new ReceiptHandleGroupKey(clientChannelInfo.getClientId(), group));
                    }
                }
            }

            @Override
            public void shutdown() {

            }
        });
    }

    protected ProxyContext createContext(String actionName) {
        return ProxyContext.createForInner(this.getClass().getSimpleName() + actionName);
    }

    protected void scheduleRenewTask() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
            for (Map.Entry<ReceiptHandleGroupKey, ReceiptHandleGroup> entry : receiptHandleGroupMap.entrySet()) {
                ReceiptHandleGroupKey key = entry.getKey();
                if (clientIsOffline(key)) {
                    clearGroup(key);
                    continue;
                }

                ReceiptHandleGroup group = entry.getValue();
                group.scan((msgID, handleStr, v) -> {
                    long current = System.currentTimeMillis();
                    ReceiptHandle handle = ReceiptHandle.decode(v.getReceiptHandleStr());
                    if (handle.getNextVisibleTime() - current > proxyConfig.getRenewAheadTimeMillis()) {
                        return;
                    }
                    renewalWorkerService.submit(() -> renewMessage(group, msgID, handleStr));
                });
            }
        } catch (Exception e) {
            log.error("unexpect error when schedule renew task", e);
        }

        log.info("scan for renewal done. cost:{}ms", stopwatch.elapsed().toMillis());
    }

    protected void renewMessage(ReceiptHandleGroup group, String msgID, String handleStr) {
        try {
            group.computeIfPresent(msgID, handleStr, this::startRenewMessage);
        } catch (Exception e) {
            log.error("error when renew message. msgID:{}, handleStr:{}", msgID, handleStr, e);
        }
    }

    // 启动对指定消息句柄的“续期（Renew）”操作。
    // 当消费者正在处理一条耗时较长的消息时，需要周期性延长该消息的“可见性超时时间”
    // 以防止消息被其他消费者重新拉取。
    // 此方法负责发起一次续期请求，并根据结果更新句柄或决定是否重试。
    protected CompletableFuture<MessageReceiptHandle> startRenewMessage(MessageReceiptHandle messageReceiptHandle) {
        // 创建一个用于异步返回的结果 future
        CompletableFuture<MessageReceiptHandle> resFuture = new CompletableFuture<>();
        // 获取全局 Proxy 配置（包含续期时间、最大重试次数等参数）
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        // 创建当前操作的上下文对象，用于链路追踪、日志标识等
        ProxyContext context = createContext("RenewMessage");
        // 解析消息句柄字符串为 ReceiptHandle 对象（包含消息位置信息、可见性时间等）
        ReceiptHandle handle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr());
        // 获取当前系统时间，用于计算是否在预期可见性窗口内
        long current = System.currentTimeMillis();
        try {
            // 检查重试次数是否已超过最大限制
            if (messageReceiptHandle.getRenewRetryTimes() >= proxyConfig.getMaxRenewRetryTimes()) {
                log.warn("handle has exceed max renewRetryTimes. handle:{}", messageReceiptHandle);
                // 达到重试上限则直接停止续期
                return CompletableFuture.completedFuture(null);
            }
            // 判断是否仍在“可见性时间窗口”内（即是否需要续期）
            if (current - messageReceiptHandle.getTimestamp() < messageReceiptHandle.getExpectInvisibleTime()) {
                // 在预期时间内 → 正常续期流程

                // 发起续期请求：调用 messagingProcessor.changeInvisibleTime
                // 参数：上下文、句柄、消息ID、消费组、主题、续期时间片（renewSliceTimeMillis）
                CompletableFuture<AckResult> future =
                    messagingProcessor.changeInvisibleTime(context, handle, messageReceiptHandle.getMessageId(),
                        messageReceiptHandle.getGroup(), messageReceiptHandle.getTopic(), proxyConfig.getRenewSliceTimeMillis());
                // 异步处理续期结果
                future.whenComplete((ackResult, throwable) -> {
                    // --- 续期异常 ---
                    if (throwable != null) {
                        log.error("error when renew. handle:{}", messageReceiptHandle, throwable);
                        // 如果异常类型可重试，则增加重试计数并返回原句柄
                        if (renewExceptionNeedRetry(throwable)) {
                            messageReceiptHandle.incrementAndGetRenewRetryTimes();
                            resFuture.complete(messageReceiptHandle);
                        } else {
                            // 不可重试的异常直接终止续期
                            resFuture.complete(null);
                        }
                    } else if (AckStatus.OK.equals(ackResult.getStatus())) {
                        // --- 续期成功 ---

                        // 更新 ReceiptHandle（Broker 返回了新的句柄）
                        messageReceiptHandle.updateReceiptHandle(ackResult.getExtraInfo());
                        // 重置重试计数
                        messageReceiptHandle.resetRenewRetryTimes();
                        // 返回更新后的句柄
                        resFuture.complete(messageReceiptHandle);
                    } else {
                        log.error("renew response is not ok. result:{}, handle:{}", ackResult, messageReceiptHandle, throwable);
                        resFuture.complete(null);
                    }
                });
            } else {
                // 否则（超出预期可见性时间窗口）

                // 表示消息可能已接近或超过可见性过期，需要进行“补偿处理”
                // 获取消费组配置，以便根据重试策略设置新的可见性时间
                SubscriptionGroupConfig subscriptionGroupConfig =
                    messagingProcessor.getMetadataService().getSubscriptionGroupConfig(messageReceiptHandle.getGroup());

                // 如果消费组配置缺失，则无法进行续期，直接终止
                if (subscriptionGroupConfig == null) {
                    log.error("group's subscriptionGroupConfig is null when renew. handle: {}", messageReceiptHandle);
                    return CompletableFuture.completedFuture(null);
                }

                // 获取该组的重试策略（RetryPolicy）
                RetryPolicy retryPolicy = subscriptionGroupConfig.getGroupRetryPolicy().getRetryPolicy();
                // 按照重试次数，计算下一次的延迟可见时间
                CompletableFuture<AckResult> future = messagingProcessor.changeInvisibleTime(context,
                    handle, messageReceiptHandle.getMessageId(), messageReceiptHandle.getGroup(),
                    messageReceiptHandle.getTopic(), retryPolicy.nextDelayDuration(messageReceiptHandle.getReconsumeTimes()));
                // 异步处理补偿结果（这里不做重试，只记录错误）
                future.whenComplete((ackResult, throwable) -> {
                    if (throwable != null) {
                        log.error("error when nack in renew. handle:{}", messageReceiptHandle, throwable);
                    }
                    resFuture.complete(null);
                });
            }
        } catch (Throwable t) {
            // 捕获所有未预料的异常，保证不会中断整个续期调度线程
            log.error("unexpect error when renew message, stop to renew it. handle:{}", messageReceiptHandle, t);
            resFuture.complete(null);
        }
        return resFuture;
    }

    protected boolean renewExceptionNeedRetry(Throwable t) {
        t = ExceptionUtils.getRealException(t);
        if (t instanceof ProxyException) {
            ProxyException proxyException = (ProxyException) t;
            if (ProxyExceptionCode.INVALID_BROKER_NAME.equals(proxyException.getCode()) ||
                ProxyExceptionCode.INVALID_RECEIPT_HANDLE.equals(proxyException.getCode())) {
                return false;
            }
        }
        return true;
    }

    protected boolean clientIsOffline(ReceiptHandleGroupKey groupKey) {
        return this.messagingProcessor.findConsumerChannel(createContext("JudgeClientOnline"), groupKey.group, groupKey.clientId) == null;
    }

    public void addReceiptHandle(String clientID, String group, String msgID, String receiptHandle,
        MessageReceiptHandle messageReceiptHandle) {
        this.addReceiptHandle(new ReceiptHandleGroupKey(clientID, group), msgID, receiptHandle, messageReceiptHandle);
    }

    protected void addReceiptHandle(ReceiptHandleGroupKey key, String msgID, String receiptHandle,
        MessageReceiptHandle messageReceiptHandle) {
        if (key == null) {
            return;
        }
        ConcurrentHashMapUtils.computeIfAbsent(this.receiptHandleGroupMap, key,
            k -> new ReceiptHandleGroup()).put(msgID, receiptHandle, messageReceiptHandle);
    }

    public MessageReceiptHandle removeReceiptHandle(String clientID, String group, String msgID, String receiptHandle) {
        return this.removeReceiptHandle(new ReceiptHandleGroupKey(clientID, group), msgID, receiptHandle);
    }

    protected MessageReceiptHandle removeReceiptHandle(ReceiptHandleGroupKey key, String msgID, String receiptHandle) {
        if (key == null) {
            return null;
        }
        ReceiptHandleGroup handleGroup = receiptHandleGroupMap.get(key);
        if (handleGroup == null) {
            return null;
        }
        return handleGroup.remove(msgID, receiptHandle);
    }

    // 清理指定 ReceiptHandleGroupKey 对应的句柄组。
    // 通常在消费组下线、客户端关闭、或消息句柄过期时调用。
    // 主要作用是：在清理句柄缓存前，尝试将这些消息的可见性时间（InvisibleTime）重置，
    // 防止因为客户端异常关闭导致消息长时间不可见，从而阻塞其他消费者消费。
    protected void clearGroup(ReceiptHandleGroupKey key) {
        // 如果 key 为空，直接返回（防御式检查）
        if (key == null) {
            return;
        }
        // 获取代理配置（ProxyConfig）实例，用于读取清理时可见性时间配置
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        // 创建当前操作上下文（ProxyContext），用于日志追踪和链路标识
        ProxyContext context = createContext("ClearGroup");
        // 从缓存映射表中移除该 group 对应的 ReceiptHandleGroup 移除后不再管理该组内的消息句柄
        ReceiptHandleGroup handleGroup = receiptHandleGroupMap.remove(key);
        if (handleGroup == null) {
            return;
        }
        // 遍历该组中所有消息句柄，执行清理逻辑
        handleGroup.scan((msgID, handle, v) -> {
            try {
                // 尝试在句柄组中对每个消息执行 computeIfPresent 操作
                // 确保句柄存在时执行后续动作
                handleGroup.computeIfPresent(msgID, handle, messageReceiptHandle -> {
                    // 解析 ReceiptHandle 字符串为 ReceiptHandle 对象
                    ReceiptHandle receiptHandle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr());

                    // 调用 messagingProcessor.changeInvisibleTime()
                    // 将该消息的可见性时间重置为配置中的清理时可见性时长
                    // 作用：让这些仍处于“锁定”状态的消息提前恢复可见，避免消费卡死
                    messagingProcessor.changeInvisibleTime(
                        context,
                        receiptHandle,
                        messageReceiptHandle.getMessageId(),
                        messageReceiptHandle.getGroup(),
                        messageReceiptHandle.getTopic(),
                        proxyConfig.getInvisibleTimeMillisWhenClear() // 新的可见性时长（通常较短）
                    );
                    return CompletableFuture.completedFuture(null);
                });
            } catch (Exception e) {
                // 捕获异常，防止单个句柄处理失败影响整个清理过程
                log.error("error when clear handle for group. key:{}", key, e);
            }
        });
    }

    protected void clearAllHandle() {
        log.info("start clear all handle in receiptHandleProcessor");
        Set<ReceiptHandleGroupKey> keySet = receiptHandleGroupMap.keySet();
        for (ReceiptHandleGroupKey key : keySet) {
            clearGroup(key);
        }
        log.info("clear all handle in receiptHandleProcessor done");
    }

    public static class ReceiptHandleGroupKey {
        private final String clientId;
        private final String group;

        public ReceiptHandleGroupKey(String clientId, String group) {
            this.clientId = clientId;
            this.group = group;
        }

        public String getClientId() {
            return clientId;
        }

        public String getGroup() {
            return group;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ReceiptHandleGroupKey key = (ReceiptHandleGroupKey) o;
            return Objects.equal(clientId, key.clientId) && Objects.equal(group, key.group);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(clientId, group);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("clientId", clientId)
                .add("group", group)
                .toString();
        }
    }
}
