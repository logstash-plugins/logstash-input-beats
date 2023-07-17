package org.logstash.beats;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class FlowLimiterHandlerTest {

    private ByteBuf prepareSample(int numBytes) {
        return prepareSample(numBytes, 'A');
    }

    private static ByteBuf prepareSample(int numBytes, char c) {
        ByteBuf payload = PooledByteBufAllocator.DEFAULT.directBuffer(numBytes);
        for (int i = 0; i < numBytes; i++) {
            payload.writeByte(c);
        }
        return payload;
    }


    boolean anyOtherCall = false;
    @Test
    public void testIntegration() throws Exception {
        final int highWaterMark = 32 * 1024;
        FlowLimiterHandler sut = new FlowLimiterHandler();

        NioEventLoopGroup group = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        try {
            b.group(group)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.config().setWriteBufferHighWaterMark(highWaterMark);
                            ch.pipeline()
                                    .addLast(new ChannelInboundHandlerAdapter() {
                                        @Override
                                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                            super.channelActive(ctx);
                                            System.out.println("Client connected");
                                            // write as much to move the channel in not writable state
                                            final ByteBuf payload = prepareSample(highWaterMark, 'C');
                                            ByteBuf copy = payload.copy();
                                            int readableBytes = copy.readableBytes();
                                            ctx.pipeline().writeAndFlush(copy).addListener(new ChannelFutureListener() {
                                                @Override
                                                public void operationComplete(ChannelFuture future) throws Exception {
                                                    System.out.println("Bytes to be read: " + readableBytes + " now availables:" + copy.readableBytes());
                                                }
                                            });
//                                            System.out.println("Bytes to be read: " + readableBytes + " now:" + copy.readableBytes());
//                                            payload.release();

                                            System.out.println("Channel isWritable?" + ctx.channel().isWritable());
                                            int numBuffers = 0;
                                            while (ctx.channel().isWritable()) {
                                                ctx.pipeline().writeAndFlush(payload.copy());
                                                numBuffers ++;
                                            }
                                            System.out.println("Num buffers wrote to get the writable false: " + numBuffers);
                                            System.out.println("Channel isWritable? " + ctx.channel().isWritable() + " high water mark: " + ctx.channel().config().getWriteBufferHighWaterMark());
                                            // ask the client to send some data present on the channel
                                            int receiveBufferSize = ((SocketChannelConfig) ctx.channel().config()).getReceiveBufferSize();
                                            System.out.println("Server's receive buffer size: " + receiveBufferSize);
                                            clientChannel.writeAndFlush(prepareSample(32));
                                        }
                                    })
                                    .addLast(sut)
                                    .addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                        boolean firstChunkRead = false;

                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                                            System.out.println("Last handler Buffer read size: " + msg.readableBytes());
                                            if (!firstChunkRead) {
                                                assertEquals("Expect to read a first chunk and no others", 32, msg.readableBytes());
                                                firstChunkRead = true;

                                                // client write other data that MUSTN'T be read by the server, because
                                                // is in rate limiting.
                                                clientChannel.writeAndFlush(prepareSample(16)).addListener(new ChannelFutureListener() {
                                                    @Override
                                                    public void operationComplete(ChannelFuture future) throws Exception {
                                                        // on successful flush schedule a shutdown
                                                        ctx.channel().eventLoop().schedule(new Runnable() {
                                                            @Override
                                                            public void run() {
                                                             group.shutdownGracefully();
                                                            }
                                                        }, 2, TimeUnit.SECONDS);
                                                    }
                                                });

                                            } else {
                                                // the first read happened, no other reads are commanded by the server
                                                // should never pass here
                                                anyOtherCall = true;
                                            }
                                        }
                                    });
                        }
                    });
            ChannelFuture future = b.bind("0.0.0.0", 1234).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        startAClient(group);
                    }
                }
            }).sync();
            future.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully().sync();
        }

        assertFalse("Shouldn't never be notified other data while in rate limiting", anyOtherCall);
    }

    Channel clientChannel;

    private void startAClient(NioEventLoopGroup group) {
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.config().setAutoRead(false);
                        clientChannel = ch;
                    }
                });
        b.connect("localhost", 1234);
    }

}