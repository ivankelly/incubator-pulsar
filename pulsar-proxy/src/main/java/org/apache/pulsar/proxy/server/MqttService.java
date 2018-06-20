package org.apache.pulsar.proxy.server;

import com.google.common.util.concurrent.AbstractService;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;

import io.netty.bootstrap.ServerBootstrap;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

//import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MqttService extends AbstractService {
    private static final Logger log = LoggerFactory.getLogger(MqttService.class);

    EventLoopGroup bossGroup = null;
    EventLoopGroup workerGroup = null;

    MqttService(ProxyConfiguration config) {
        log.info("IKDEBUG creating mqtt service");
    }

    private static class MqttServerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                // Do something with msg
                log.info("GOT message {}", msg);
                if (msg instanceof MqttConnectMessage) {
                    ctx.writeAndFlush(MqttMessageBuilders.connAck()
                              .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                              .sessionPresent(false).build());
                } else if (msg instanceof MqttSubscribeMessage) {
                    MqttSubscribeMessage submsg = (MqttSubscribeMessage)msg;

                    PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

                    String topic = submsg.payload().topicSubscriptions().get(0).topicName();
                    MessageListener<byte[]> listener = (_topic, message) -> {
                        log.info("sending a message");
                        ctx.writeAndFlush(MqttMessageBuilders.publish()
                                          .topicName(topic)
                                          .retained(false)
                                          .messageId(1)
                                          .qos(MqttQoS.AT_LEAST_ONCE)
                                          .payload(Unpooled.wrappedBuffer(message.getData()))
                                          .build());
                    };
                    client.<byte[]>newConsumer()
                        .topic(topic)
                        .subscriptionName("foobar")
                        .messageListener(listener)
                        .subscribeAsync()
                        .whenComplete((res, ex) -> {
                                MqttSubAckMessage response = new MqttSubAckMessage(
                                        new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                                        MqttMessageIdVariableHeader.from(submsg.variableHeader().messageId()),
                                        new MqttSubAckPayload(MqttQoS.AT_LEAST_ONCE.value()));
                                ctx.writeAndFlush(response);
                            });
                }
            } catch (Exception e) {
                log.error("Error in handler", e);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // Close the connection when an exception is raised.
            log.error("MQTT server error", cause);
            ctx.close();
        }
    }

    @Override
    protected void doStart() {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("decoder", new MqttDecoder(5*1024*1024));
                        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
                        ch.pipeline().addLast("handler", new MqttServerHandler());
                    }
                })
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true);
        b.bind(1833);

        notifyStarted();
    }

    @Override
    protected void doStop() {
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        notifyStopped();
    }
}
