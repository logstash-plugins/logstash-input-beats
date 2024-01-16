package org.logstash.beats;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class FlowLimiterHandlerTest {

    private ReadMessagesCollector readMessagesCollector;

    private static ByteBuf prepareSample(int numBytes) {
        return prepareSample(numBytes, 'A');
    }

    private static ByteBuf prepareSample(int numBytes, char c) {
        ByteBuf payload = PooledByteBufAllocator.DEFAULT.directBuffer(numBytes);
        for (int i = 0; i < numBytes; i++) {
            payload.writeByte(c);
        }
        return payload;
    }

    private ChannelInboundHandlerAdapter onClientConnected(Consumer<ChannelHandlerContext> action) {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                action.accept(ctx);
            }
        };
    }

    private static class ReadMessagesCollector extends SimpleChannelInboundHandler<ByteBuf> {
        private Channel clientChannel;
        private final NioEventLoopGroup group;
        boolean firstChunkRead = false;

        ReadMessagesCollector(NioEventLoopGroup group) {
            this.group = group;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            if (!firstChunkRead) {
                assertEquals("Expect to read a first chunk and no others", 32, msg.readableBytes());
                firstChunkRead = true;

                // client write other data that MUSTN'T be read by the server, because
                // is rate limited.
                clientChannel.writeAndFlush(prepareSample(16)).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            // on successful flush schedule a shutdown
                            ctx.channel().eventLoop().schedule(new Runnable() {
                                @Override
                                public void run() {
                                    group.shutdownGracefully();
                                }
                            }, 2, TimeUnit.SECONDS);
                        } else {
                            ctx.fireExceptionCaught(future.cause());
                        }
                    }
                });

            } else {
                // the first read happened, no other reads are commanded by the server
                // should never pass here
                fail("Shouldn't never be notified other data while in rate limiting");
            }
        }

        public void updateClient(Channel clientChannel) {
            assertNotNull(clientChannel);
            this.clientChannel = clientChannel;
        }
    }


    private static class AssertionsHandler extends ChannelInboundHandlerAdapter {

        private final NioEventLoopGroup group;

        private Throwable lastError;

        public AssertionsHandler(NioEventLoopGroup group) {
            this.group = group;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            lastError = cause;
            group.shutdownGracefully();
        }

        public void assertNoErrors() {
            if (lastError != null) {
                if (lastError instanceof AssertionError) {
                    throw (AssertionError) lastError;
                } else {
                    fail("Failed with error" + lastError);
                }
            }
        }
    }

    @Test
    public void givenAChannelInNotWriteableStateWhenNewBuffersAreSentByClientThenNoDecodeTakePartOnServerSide() throws Exception {
        final int highWaterMark = 32 * 1024;
        FlowLimiterHandler sut = new FlowLimiterHandler();

        NioEventLoopGroup group = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();

        readMessagesCollector = new ReadMessagesCollector(group);
        AssertionsHandler assertionsHandler = new AssertionsHandler(group);
        try {
            b.group(group)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.config().setWriteBufferHighWaterMark(highWaterMark);
                            ch.pipeline()
                                    .addLast(onClientConnected(ctx -> {
                                        // write as much to move the channel in not writable state
                                        fillOutboundWatermark(ctx, highWaterMark);
                                        // ask the client to send some data present on the channel
                                        clientChannel.writeAndFlush(prepareSample(32));
                                    }))
                                    .addLast(sut)
                                    .addLast(readMessagesCollector)
                                    .addLast(assertionsHandler);
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

        assertionsHandler.assertNoErrors();
    }

    private static void fillOutboundWatermark(ChannelHandlerContext ctx, int highWaterMark) {
        final ByteBuf payload = prepareSample(highWaterMark, 'C');
        while (ctx.channel().isWritable()) {
            ctx.pipeline().writeAndFlush(payload.copy());
        }
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
                        readMessagesCollector.updateClient(clientChannel);
                    }
                });
        b.connect("localhost", 1234);
    }

}