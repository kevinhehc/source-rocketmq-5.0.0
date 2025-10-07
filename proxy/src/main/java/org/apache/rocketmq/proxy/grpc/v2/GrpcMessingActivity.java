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

package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.StartAndShutdown;

public interface GrpcMessingActivity extends StartAndShutdown {

    //作用：
    //查询指定主题（Topic）的路由信息（Route）。
    //客户端在发送消息或拉取消息前，会通过该接口向 Proxy 请求主题的路由配置（包含可用的 Broker、队列、读写权限等信息）。
    //
    //典型用途： producer/consumer 启动时更新路由表。
    //
    //参数说明：
    //	•	ProxyContext ctx：上下文信息（包括身份、追踪、客户端信息等）。
    //	•	QueryRouteRequest request：包含要查询的主题名。
    //	•	返回值为异步的 QueryRouteResponse。
    CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request);

    //作用：
    //接收客户端心跳，保持连接与注册信息的活性。
    //
    //Consumer、Producer 客户端都会周期性地向 Proxy 发送心跳，以维持与 Broker 的连接注册。
    //
    //典型场景： 客户端维持在线状态、更新消费组成员信息等。
    CompletableFuture<HeartbeatResponse> heartbeat(ProxyContext ctx, HeartbeatRequest request);

    //作用：
    //处理客户端发送消息请求。
    //Proxy 收到消息后，会根据路由表选择合适的 Broker，并将消息转发给 Broker。
    //
    //典型场景： Producer 通过 gRPC 向 Proxy 发送普通、延时、事务消息。
    CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request);

    //作用：
    //查询消费分配结果（Assignment）。
    //用于消费者负载均衡（Rebalance）后，确定当前客户端被分配到哪些队列。
    //
    //典型场景： PushConsumer 拉取前更新消费队列分配信息。
    CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx, QueryAssignmentRequest request);

    //作用：
    //处理客户端的拉取消息请求（Receive Message）。
    //Proxy 会调用 Broker 获取消息，并通过 StreamObserver 以流式异步响应返回消息。
    //
    //特点：
    //	•	这是一个 server-streaming 接口（服务端持续推送消息流）。
    //	•	支持长轮询（long polling）或流式消费模式。
    //
    //典型场景： PushConsumer 接收消息。
    void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver);

    //作用：
    //客户端消费完成后，通过此接口向 Proxy 确认（ACK）消息已成功处理。
    //Proxy 再将 ACK 转发给 Broker，Broker 将消息从队列中标记为“已消费”。
    //
    //典型场景： 消费者业务处理成功后提交 ACK。
    CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request);

    //作用：
    //将无法成功消费的消息转发到 死信队列（DLQ, Dead Letter Queue）。
    //当消息重试次数达到上限或消费逻辑失败时，消费者通过该接口将消息推送到 DLQ。
    //
    //典型场景： 消费异常或消息处理失败的兜底机制。
    CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(ProxyContext ctx,
        ForwardMessageToDeadLetterQueueRequest request);

    //作用：
    //处理事务消息的提交或回滚请求。
    //Producer 在半消息发送成功后，通过此接口告诉 Broker 事务状态（提交 / 回滚）。
    //
    //典型场景： RocketMQ 事务消息（Transactional Message）的第二阶段。
    CompletableFuture<EndTransactionResponse> endTransaction(ProxyContext ctx, EndTransactionRequest request);

    //作用：
    //通知 Proxy 某个客户端已正常关闭或断开连接。
    //Proxy 会清理该客户端相关的资源（例如消费分配、心跳信息、缓存句柄等）。
    //
    //典型场景： 客户端优雅退出（shutdown）时调用。
    CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(ProxyContext ctx,
        NotifyClientTerminationRequest request);

    //作用：
    //修改消息的可见性超时时间（Invisible Duration）。
    //消费者在处理耗时较长的消息时，可以通过此接口延长消息的“锁定时间”，防止被其他消费者重新消费。
    //
    //典型场景： 长任务消费、动态延时处理。
    CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(ProxyContext ctx,
        ChangeInvisibleDurationRequest request);

    //作用：
    //建立一个双向流式（bi-directional streaming） gRPC 通道，用于客户端与 Proxy 之间进行实时通信。
    //
    //用途包括：
    //	•	上报客户端指标（metrics）
    //	•	实时健康状态
    //	•	动态配置下发（如重平衡指令、订阅变更）
    //
    //返回值：
    //一个 StreamObserver，客户端可持续发送 TelemetryCommand，Proxy 也可主动下发命令。
    StreamObserver<TelemetryCommand> telemetry(ProxyContext ctx, StreamObserver<TelemetryCommand> responseObserver);
}
