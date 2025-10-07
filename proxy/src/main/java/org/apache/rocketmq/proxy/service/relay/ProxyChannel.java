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

package org.apache.rocketmq.proxy.service.relay;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public abstract class ProxyChannel extends SimpleChannel {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected final SocketAddress remoteSocketAddress;
    protected final SocketAddress localSocketAddress;

    protected final ProxyRelayService proxyRelayService;

    protected ProxyChannel(ProxyRelayService proxyRelayService, Channel parent, String remoteAddress,
        String localAddress) {
        super(parent, remoteAddress, localAddress);
        this.proxyRelayService = proxyRelayService;
        this.remoteSocketAddress = RemotingUtil.string2SocketAddress(remoteAddress);
        this.localSocketAddress = RemotingUtil.string2SocketAddress(localAddress);
    }

    protected ProxyChannel(ProxyRelayService proxyRelayService, Channel parent, ChannelId id, String remoteAddress,
        String localAddress) {
        super(parent, id, remoteAddress, localAddress);
        this.proxyRelayService = proxyRelayService;
        this.remoteSocketAddress = RemotingUtil.string2SocketAddress(remoteAddress);
        this.localSocketAddress = RemotingUtil.string2SocketAddress(localAddress);
    }

    /**
     * 处理并发送消息的核心方法（Proxy 侧转发逻辑）。
     *
     * 在 RocketMQ Proxy 的集群模式中，该方法负责：
     *  1. 接收上层传入的消息对象（通常是 RemotingCommand）；
     *  2. 根据消息类型（RequestCode）判断请求类型；
     *  3. 执行对应的业务处理逻辑（例如事务检查、消费状态查询等）；
     *  4. 异步处理完成后返回一个 Netty 的 ChannelFuture（用于通知结果）。
     *
     * 本质上，这个方法模拟了 Netty channel 的 writeAndFlush 行为，
     * 但这里的“发送”并非网络传输，而是“通过 Proxy Relay 转发或处理消息”的过程。
     *
     * @param msg  需要处理的消息对象，可能是 RemotingCommand 或其他类型
     * @return     Netty ChannelFuture，表示异步处理的结果
     */
    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        // 创建一个异步处理结果的 Future，用于异步通知处理完成
        CompletableFuture<Void> processFuture = new CompletableFuture<>();

        try {
            // 判断是否是 RocketMQ 内部协议对象 RemotingCommand
            if (msg instanceof RemotingCommand) {
                // 创建 ProxyContext，用于封装调用链信息、上下文追踪、远端/本地地址等
                ProxyContext context = ProxyContext.createForInner(this.getClass())
                    .setRemoteAddress(remoteAddress)
                    .setLocalAddress(localAddress);
                RemotingCommand command = (RemotingCommand) msg;
                // 如果扩展字段为空，则初始化一个空的 map
                if (command.getExtFields() == null) {
                    command.setExtFields(new HashMap<>());
                }
                // 根据命令类型（RequestCode）分支处理不同业务逻辑
                switch (command.getCode()) {
                    // 事务状态检查请求（CheckTransactionState）
                    case RequestCode.CHECK_TRANSACTION_STATE: {
                        CheckTransactionStateRequestHeader header = (CheckTransactionStateRequestHeader) command.readCustomHeader();
                        // 解析消息体为 MessageExt 对象
                        MessageExt messageExt = MessageDecoder.decode(ByteBuffer.wrap(command.getBody()), true, false, false);
                        // 调用 ProxyRelayService，生成中继任务（Relay）
                        RelayData<TransactionData, Void> relayData = this.proxyRelayService.processCheckTransactionState(context, command, header, messageExt);
                        // 调用内部方法继续处理事务检查
                        processFuture = this.processCheckTransaction(header, messageExt, relayData.getProcessResult(), relayData.getRelayFuture());
                        break;
                    }
                    // 获取 Consumer 运行状态信息请求
                    case RequestCode.GET_CONSUMER_RUNNING_INFO: {
                        GetConsumerRunningInfoRequestHeader header = (GetConsumerRunningInfoRequestHeader) command.readCustomHeader();
                        // 调用 relay 服务，从远程 Proxy 拉取 Consumer 运行状态
                        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> relayFuture = this.proxyRelayService.processGetConsumerRunningInfo(context, command, header);
                        // 处理返回结果
                        processFuture = this.processGetConsumerRunningInfo(command, header, relayFuture);
                        break;
                    }
                    // 直接消费消息请求（用于测试/诊断）
                    case RequestCode.CONSUME_MESSAGE_DIRECTLY: {
                        ConsumeMessageDirectlyResultRequestHeader header = (ConsumeMessageDirectlyResultRequestHeader) command.readCustomHeader();
                        MessageExt messageExt = MessageDecoder.decode(ByteBuffer.wrap(command.getBody()), true, false, false);
                        // 异步调用 relay 服务直接消费消息
                        processFuture = this.processConsumeMessageDirectly(command, header, messageExt,
                            this.proxyRelayService.processConsumeMessageDirectly(context, command, header));
                        break;
                    }
                    // 未识别的请求类型，忽略处理
                    default:
                        break;
                }
            } else {
                // 如果不是 RemotingCommand 类型，则交由其他处理逻辑
                processFuture = processOtherMessage(msg);
            }
        } catch (Throwable t) {
            log.error("process failed. msg:{}", msg, t);
            // 捕获任何异常，打印错误日志并标记 future 异常完成
            processFuture.completeExceptionally(t);
        }

        // 构造一个 Netty 的 Promise 对象（包装异步结果）
        DefaultChannelPromise promise = new DefaultChannelPromise(this, GlobalEventExecutor.INSTANCE);
        // 当 processFuture 成功时，设置 promise 成功
        processFuture.thenAccept(ignore -> promise.setSuccess())
                // 当失败时，将异常传递到 promise
            .exceptionally(t -> {
                promise.setFailure(t);
                return null;
            });
        // 返回 Netty 风格的 ChannelFuture，以保持 API 一致性
        return promise;
    }

    protected abstract CompletableFuture<Void> processOtherMessage(Object msg);

    protected abstract CompletableFuture<Void> processCheckTransaction(
        CheckTransactionStateRequestHeader header,
        MessageExt messageExt,
        TransactionData transactionData,
        CompletableFuture<ProxyRelayResult<Void>> responseFuture);

    protected abstract CompletableFuture<Void> processGetConsumerRunningInfo(
        RemotingCommand command,
        GetConsumerRunningInfoRequestHeader header,
        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> responseFuture);

    protected abstract CompletableFuture<Void> processConsumeMessageDirectly(
        RemotingCommand command,
        ConsumeMessageDirectlyResultRequestHeader header,
        MessageExt messageExt,
        CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> responseFuture);

    @Override
    public ChannelConfig config() {
        return null;
    }

    @Override
    public ChannelMetadata metadata() {
        return null;
    }

    @Override
    protected AbstractUnsafe newUnsafe() {
        return null;
    }

    @Override
    protected boolean isCompatible(EventLoop loop) {
        return false;
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {

    }

    @Override
    protected void doDisconnect() throws Exception {

    }

    @Override
    protected void doClose() throws Exception {

    }

    @Override
    protected void doBeginRead() throws Exception {

    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {

    }

    @Override
    protected SocketAddress localAddress0() {
        return this.localSocketAddress;
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return this.remoteSocketAddress;
    }
}
