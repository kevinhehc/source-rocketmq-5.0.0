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
package org.apache.rocketmq.client.trace;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

import static org.apache.rocketmq.client.trace.TraceConstants.TRACE_INSTANCE_NAME;

public class AsyncTraceDispatcher implements TraceDispatcher {

    // 日志记录器，用于记录运行时的调试、错误等日志
    private final static InternalLogger log = ClientLogger.getLog();

    // 原子整型计数器，通常用于生成全局唯一的序号或统计次数
    private final static AtomicInteger COUNTER = new AtomicInteger();

    // 队列的最大容量，表示可缓存的 TraceContext 数量
    private final int queueSize;

    // 每次批量处理的大小，决定一次最多发送多少 trace 数据
    private final int batchSize;

    // 单条消息的最大大小限制，避免消息体过大
    private final int maxMsgSize;

    // 轮询时间（毫秒），用于线程循环时的等待间隔
    private final long pollingTimeMil;

    // 等待超时时间阈值（毫秒），超过该时间未能处理的任务可能会被丢弃
    private final long waitTimeThresholdMil;

    // 用于发送 trace 数据的 RocketMQ 生产者
    private final DefaultMQProducer traceProducer;

    // 执行 trace 任务的线程池，用来异步处理 trace 数据
    private final ThreadPoolExecutor traceExecutor;

    // 丢弃日志的次数（原子计数），用于统计被丢弃的 trace 条目数
    private AtomicLong discardCount;

    // 后台执行任务的工作线程
    private Thread worker;

    // 存放 TraceContext 的阻塞队列，用于存储待处理的 trace 数据
    private final ArrayBlockingQueue<TraceContext> traceContextQueue;

    // 按 Topic 组织的任务队列，保存每个 Topic 对应的 trace 数据分片
    private final HashMap<String, TraceDataSegment> taskQueueByTopic;

    // 任务追加的阻塞队列，通常配合线程池使用
    private ArrayBlockingQueue<Runnable> appenderQueue;

    // JVM 关闭时的钩子线程，用于资源清理
    private volatile Thread shutDownHook;

    // 停止标识位，控制是否结束工作线程
    private volatile boolean stopped = false;

    // 宿主生产者实例（宿主业务 Producer）
    private DefaultMQProducerImpl hostProducer;

    // 宿主消费者实例（宿主业务 Consumer）
    private DefaultMQPushConsumerImpl hostConsumer;

    // 选择发送到哪个队列的索引工具，保证消息均衡分布
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

    // dispatcher 的唯一标识符，用于区分不同的 trace 任务实例
    private String dispatcherId = UUID.randomUUID().toString();

    // trace 消息发送的目标 Topic 名称
    private volatile String traceTopicName;

    // 是否已启动的标识位，防止重复初始化
    private AtomicBoolean isStarted = new AtomicBoolean(false);

    // 访问通道类型（LOCAL、CLOUD 等），决定消息投递路径
    private volatile AccessChannel accessChannel = AccessChannel.LOCAL;

    // 消费组名，用于标识 trace 任务所属的 Group
    private String group;

    // 类型（生产者 / 消费者），用于区分 trace 的来源
    private Type type;

    public AsyncTraceDispatcher(String group, Type type, String traceTopicName, RPCHook rpcHook) {
        // queueSize is greater than or equal to the n power of 2 of value
        this.queueSize = 2048;
        this.batchSize = 100;
        this.maxMsgSize = 128000;
        this.pollingTimeMil = 100;
        this.waitTimeThresholdMil = 500;
        this.discardCount = new AtomicLong(0L);
        this.traceContextQueue = new ArrayBlockingQueue<TraceContext>(1024);
        this.taskQueueByTopic = new HashMap();
        this.group = group;
        this.type = type;

        this.appenderQueue = new ArrayBlockingQueue<Runnable>(queueSize);
        if (!UtilAll.isBlank(traceTopicName)) {
            this.traceTopicName = traceTopicName;
        } else {
            this.traceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;
        }
        this.traceExecutor = new ThreadPoolExecutor(//
            10, //
            20, //
            1000 * 60, //
            TimeUnit.MILLISECONDS, //
            this.appenderQueue, //
            new ThreadFactoryImpl("MQTraceSendThread_"));
        traceProducer = getAndCreateTraceProducer(rpcHook);
    }

    public AccessChannel getAccessChannel() {
        return accessChannel;
    }

    public void setAccessChannel(AccessChannel accessChannel) {
        this.accessChannel = accessChannel;
    }

    public String getTraceTopicName() {
        return traceTopicName;
    }

    public void setTraceTopicName(String traceTopicName) {
        this.traceTopicName = traceTopicName;
    }

    public DefaultMQProducer getTraceProducer() {
        return traceProducer;
    }

    public DefaultMQProducerImpl getHostProducer() {
        return hostProducer;
    }

    public void setHostProducer(DefaultMQProducerImpl hostProducer) {
        this.hostProducer = hostProducer;
    }

    public DefaultMQPushConsumerImpl getHostConsumer() {
        return hostConsumer;
    }

    public void setHostConsumer(DefaultMQPushConsumerImpl hostConsumer) {
        this.hostConsumer = hostConsumer;
    }

    public void start(String nameSrvAddr, AccessChannel accessChannel) throws MQClientException {
        if (isStarted.compareAndSet(false, true)) {
            traceProducer.setNamesrvAddr(nameSrvAddr);
            traceProducer.setInstanceName(TRACE_INSTANCE_NAME + "_" + nameSrvAddr);
            traceProducer.start();
        }
        this.accessChannel = accessChannel;
        this.worker = new Thread(new AsyncRunnable(), "MQ-AsyncTraceDispatcher-Thread-" + dispatcherId);
        this.worker.setDaemon(true);
        this.worker.start();
        this.registerShutDownHook();
    }

    private DefaultMQProducer getAndCreateTraceProducer(RPCHook rpcHook) {
        DefaultMQProducer traceProducerInstance = this.traceProducer;
        if (traceProducerInstance == null) {
            traceProducerInstance = new DefaultMQProducer(rpcHook);
            traceProducerInstance.setProducerGroup(genGroupNameForTrace());
            traceProducerInstance.setSendMsgTimeout(5000);
            traceProducerInstance.setVipChannelEnabled(false);
            // The max size of message is 128K
            traceProducerInstance.setMaxMessageSize(maxMsgSize);
        }
        return traceProducerInstance;
    }

    private String genGroupNameForTrace() {
        return TraceConstants.GROUP_NAME_PREFIX + "-" + this.group + "-" + this.type + "-" + COUNTER.incrementAndGet();
    }

    @Override
    public boolean append(final Object ctx) {
        boolean result = traceContextQueue.offer((TraceContext) ctx);
        if (!result) {
            log.info("buffer full" + discardCount.incrementAndGet() + " ,context is " + ctx);
        }
        return result;
    }

    @Override
    public void flush() {
        // The maximum waiting time for refresh,avoid being written all the time, resulting in failure to return.
        long end = System.currentTimeMillis() + 500;
        while (System.currentTimeMillis() <= end) {
            synchronized (taskQueueByTopic) {
                for (TraceDataSegment taskInfo : taskQueueByTopic.values()) {
                    taskInfo.sendAllData();
                }
            }
            synchronized (traceContextQueue) {
                if (traceContextQueue.size() == 0 && appenderQueue.size() == 0) {
                    break;
                }
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                break;
            }
        }
        log.info("------end trace send " + traceContextQueue.size() + "   " + appenderQueue.size());
    }

    @Override
    public void shutdown() {
        this.stopped = true;
        flush();
        this.traceExecutor.shutdown();
        if (isStarted.get()) {
            traceProducer.shutdown();
        }
        this.removeShutdownHook();
    }

    public void registerShutDownHook() {
        if (shutDownHook == null) {
            shutDownHook = new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;

                @Override
                public void run() {
                    synchronized (this) {
                        if (!this.hasShutdown) {
                            flush();
                        }
                    }
                }
            }, "ShutdownHookMQTrace");
            Runtime.getRuntime().addShutdownHook(shutDownHook);
        }
    }

    public void removeShutdownHook() {
        if (shutDownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutDownHook);
            } catch (IllegalStateException e) {
                // ignore - VM is already shutting down
            }
        }
    }

    class AsyncRunnable implements Runnable {
        private boolean stopped;

        @Override
        public void run() {
            while (!stopped) {
                synchronized (traceContextQueue) {
                    long endTime = System.currentTimeMillis() + pollingTimeMil;
                    while (System.currentTimeMillis() < endTime) {
                        try {
                            TraceContext traceContext = traceContextQueue.poll(
                                endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS
                            );

                            if (traceContext != null && !traceContext.getTraceBeans().isEmpty()) {
                                // get the topic which the trace message will send to
                                String traceTopicName = this.getTraceTopicName(traceContext.getRegionId());

                                // get the traceDataSegment which will save this trace message, create if null
                                TraceDataSegment traceDataSegment = taskQueueByTopic.get(traceTopicName);
                                if (traceDataSegment == null) {
                                    traceDataSegment = new TraceDataSegment(traceTopicName, traceContext.getRegionId());
                                    taskQueueByTopic.put(traceTopicName, traceDataSegment);
                                }

                                // encode traceContext and save it into traceDataSegment
                                // NOTE if data size in traceDataSegment more than maxMsgSize,
                                //  a AsyncDataSendTask will be created and submitted
                                TraceTransferBean traceTransferBean = TraceDataEncoder.encoderFromContextBean(traceContext);
                                traceDataSegment.addTraceTransferBean(traceTransferBean);
                            }
                        } catch (InterruptedException ignore) {
                            log.debug("traceContextQueue#poll exception");
                        }
                    }

                    // NOTE send the data in traceDataSegment which the first TraceTransferBean
                    //  is longer than waitTimeThreshold
                    sendDataByTimeThreshold();

                    if (AsyncTraceDispatcher.this.stopped) {
                        this.stopped = true;
                    }
                }
            }

        }

        private void sendDataByTimeThreshold() {
            long now = System.currentTimeMillis();
            for (TraceDataSegment taskInfo : taskQueueByTopic.values()) {
                if (now - taskInfo.firstBeanAddTime >= waitTimeThresholdMil) {
                    taskInfo.sendAllData();
                }
            }
        }

        private String getTraceTopicName(String regionId) {
            AccessChannel accessChannel = AsyncTraceDispatcher.this.getAccessChannel();
            if (AccessChannel.CLOUD == accessChannel) {
                return TraceConstants.TRACE_TOPIC_PREFIX + regionId;
            }

            return AsyncTraceDispatcher.this.getTraceTopicName();
        }
    }

    class TraceDataSegment {
        private long firstBeanAddTime;
        private int currentMsgSize;
        private final String traceTopicName;
        private final String regionId;
        private final List<TraceTransferBean> traceTransferBeanList = new ArrayList();

        TraceDataSegment(String traceTopicName, String regionId) {
            this.traceTopicName = traceTopicName;
            this.regionId = regionId;
        }

        public void addTraceTransferBean(TraceTransferBean traceTransferBean) {
            initFirstBeanAddTime();
            this.traceTransferBeanList.add(traceTransferBean);
            this.currentMsgSize += traceTransferBean.getTransData().length();
            if (currentMsgSize >= traceProducer.getMaxMessageSize() - 10 * 1000) {
                List<TraceTransferBean> dataToSend = new ArrayList(traceTransferBeanList);
                AsyncDataSendTask asyncDataSendTask = new AsyncDataSendTask(traceTopicName, regionId, dataToSend);
                traceExecutor.submit(asyncDataSendTask);

                this.clear();

            }
        }

        public void sendAllData() {
            if (this.traceTransferBeanList.isEmpty()) {
                return;
            }
            List<TraceTransferBean> dataToSend = new ArrayList(traceTransferBeanList);
            AsyncDataSendTask asyncDataSendTask = new AsyncDataSendTask(traceTopicName, regionId, dataToSend);
            traceExecutor.submit(asyncDataSendTask);

            this.clear();
        }

        private void initFirstBeanAddTime() {
            if (firstBeanAddTime == 0) {
                firstBeanAddTime = System.currentTimeMillis();
            }
        }

        private void clear() {
            this.firstBeanAddTime = 0;
            this.currentMsgSize = 0;
            this.traceTransferBeanList.clear();
        }
    }


    class AsyncDataSendTask implements Runnable {
        private final String traceTopicName;
        private final String regionId;
        private final List<TraceTransferBean> traceTransferBeanList;

        public AsyncDataSendTask(String traceTopicName, String regionId, List<TraceTransferBean> traceTransferBeanList) {
            this.traceTopicName = traceTopicName;
            this.regionId = regionId;
            this.traceTransferBeanList = traceTransferBeanList;
        }

        @Override
        public void run() {
            StringBuilder buffer = new StringBuilder(1024);
            Set<String> keySet = new HashSet<String>();
            for (TraceTransferBean bean : traceTransferBeanList) {
                keySet.addAll(bean.getTransKey());
                buffer.append(bean.getTransData());
            }
            sendTraceDataByMQ(keySet, buffer.toString(), traceTopicName);
        }

        /**
         * Send message trace data
         *
         * @param keySet the keyset in this batch(including msgId in original message not offsetMsgId)
         * @param data   the message trace data in this batch
         * @param traceTopic the topic which message trace data will send to
         */
        private void sendTraceDataByMQ(Set<String> keySet, final String data, String traceTopic) {
            final Message message = new Message(traceTopic, data.getBytes(StandardCharsets.UTF_8));
            // Keyset of message trace includes msgId of or original message
            message.setKeys(keySet);
            try {
                Set<String> traceBrokerSet = tryGetMessageQueueBrokerSet(traceProducer.getDefaultMQProducerImpl(), traceTopic);
                SendCallback callback = new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {

                    }

                    @Override
                    public void onException(Throwable e) {
                        log.error("send trace data failed, the traceData is {}", data, e);
                    }
                };
                if (traceBrokerSet.isEmpty()) {
                    // No cross set
                    traceProducer.send(message, callback, 5000);
                } else {
                    traceProducer.send(message, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            Set<String> brokerSet = (Set<String>) arg;
                            List<MessageQueue> filterMqs = new ArrayList<MessageQueue>();
                            for (MessageQueue queue : mqs) {
                                if (brokerSet.contains(queue.getBrokerName())) {
                                    filterMqs.add(queue);
                                }
                            }
                            int index = sendWhichQueue.incrementAndGet();
                            int pos = Math.abs(index) % filterMqs.size();
                            if (pos < 0) {
                                pos = 0;
                            }
                            return filterMqs.get(pos);
                        }
                    }, traceBrokerSet, callback);
                }

            } catch (Exception e) {
                log.error("send trace data failed, the traceData is {}", data, e);
            }
        }

        private Set<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
            Set<String> brokerSet = new HashSet<String>();
            TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            if (null == topicPublishInfo || !topicPublishInfo.ok()) {
                producer.getTopicPublishInfoTable().putIfAbsent(topic, new TopicPublishInfo());
                producer.getMqClientFactory().updateTopicRouteInfoFromNameServer(topic);
                topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            }
            if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
                for (MessageQueue queue : topicPublishInfo.getMessageQueueList()) {
                    brokerSet.add(queue.getBrokerName());
                }
            }
            return brokerSet;
        }
    }

}
