package org.logstash.beats;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.number.IsCloseTo.closeTo;


public class ServerTest {
    private int randomPort;
    private EventLoopGroup group;
    private final String host = "0.0.0.0";

    @Before
    public void setUp() {
        randomPort = ThreadLocalRandom.current().nextInt(1024, 65535);
        group = new NioEventLoopGroup();
    }

    @Test
    public void testServerShouldTerminateConnectionWhenExceptionHappen() throws InterruptedException {
        int inactivityTime = 3; // in seconds
        int concurrentConnections = 10;

        final Random random = new Random();

        final CountDownLatch latch = new CountDownLatch(concurrentConnections);

        final Server server = new Server(host, randomPort, inactivityTime);
        final AtomicBoolean otherCause = new AtomicBoolean(false);
        server.setMessageListener(new MessageListener() {
            public void onNewConnection(ChannelHandlerContext ctx) {
                // Make sure connection is closed on exception too.
                if (random.nextInt(10) < 1) {
                    throw new RuntimeException("Dummy");
                }
            }

            @Override
            public void onConnectionClose(ChannelHandlerContext ctx) {
                latch.countDown();
            }

            @Override
            public void onNewMessage(ChannelHandlerContext ctx, Message message) {
                // Make sure connection is closed on exception too.
                throw new RuntimeException("Dummy");
            }

            @Override
            public void onException(ChannelHandlerContext ctx, Throwable cause) {
                // Make sure only intended exception is thrown
                if (!"Dummy".equals(cause.getMessage())) {
                    otherCause.set(true);
                }
            }
        });

        Runnable serverTask = new Runnable() {
            @Override
            public void run() {
                try {
                    server.listen();
                } catch (InterruptedException e) {
                }
            }
        };

        Thread thread = new Thread(serverTask);
        thread.start();
        sleep(1000); // give some time to travis..

        try {
            for (int i = 0; i < concurrentConnections; i++) {
                connectClient();
            }
            assertThat(latch.await(10, TimeUnit.SECONDS), is(true));
            assertThat(otherCause.get(), is(false));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testServerShouldTerminateConnectionIdleForTooLong() throws InterruptedException {
        int inactivityTime = 3; // in seconds
        int concurrentConnections = 10;

        final Random random = new Random();

        final CountDownLatch latch = new CountDownLatch(concurrentConnections);
        final AtomicBoolean exceptionClose = new AtomicBoolean(false);
        final Server server = new Server(host, randomPort, inactivityTime);
        server.setMessageListener(new MessageListener() {
            @Override
            public void onNewConnection(ChannelHandlerContext ctx) {
            }

            @Override
            public void onConnectionClose(ChannelHandlerContext ctx) {
                latch.countDown();
            }

            @Override
            public void onNewMessage(ChannelHandlerContext ctx, Message message) {
            }

            @Override
            public void onException(ChannelHandlerContext ctx, Throwable cause) {
                    exceptionClose.set(true);
            }
        });

        Runnable serverTask = new Runnable() {
            @Override
            public void run() {
                try {
                    server.listen();
                } catch (InterruptedException e) {
                }
            }
        };

        Thread thread = new Thread(serverTask);
        thread.start();
        sleep(1000); // give some time to travis..

        try {
            long started = System.currentTimeMillis();


            for (int i = 0; i < concurrentConnections; i++) {
                connectClient();
            }
            assertThat(latch.await(10, TimeUnit.SECONDS), is(true));

            long ended = System.currentTimeMillis();

            long diff = ended - started;
            assertThat(diff/1000.0, is(closeTo(inactivityTime, .5)));
            assertThat(exceptionClose.get(), is(false));
        } finally {
            server.stop();
        }
    }

    @Test
    public void testServerShouldAcceptConcurrentConnection() throws InterruptedException {
        final Server server = new Server(host, randomPort, 30);
        SpyListener listener = new SpyListener();
        server.setMessageListener(listener);
        Runnable serverTask = new Runnable() {
            @Override
            public void run() {
                try {
                    server.listen();
                } catch (InterruptedException e) {
                }
            }
        };

        new Thread(serverTask).start();
        sleep(1000); // start server give is some time.

        // Each connection is sending 1 batch.
        int ConcurrentConnections = 5;

        for(int i = 0; i < ConcurrentConnections; i++) {
            Runnable clientTask = new Runnable(){

                @Override
                public void run() {
                    try {

                        connectClient().addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                            }
                        });

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };

            new Thread(clientTask).start();
        }
        // HACK: I didn't not find a nice solutions to test if the connection was still
        // open on the client without actually sending data down the wire.
        int iteration = 0;
        int maxIteration = 30;


        while(listener.getReceivedCount() < ConcurrentConnections) {
            Thread.sleep(1000);
            iteration++;

            if (iteration >= maxIteration) {
                System.out.println("reached max iteration" + maxIteration);
                break;
            }

        }
        assertThat(listener.getReceivedCount(), is(ConcurrentConnections));

        group.shutdownGracefully();
        server.stop();

    }

    public ChannelFuture connectClient() throws InterruptedException {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                                 @Override
                                 public void initChannel(SocketChannel ch) throws Exception {
                                     ChannelPipeline pipeline = ch.pipeline();
                                     pipeline.addLast(new BatchEncoder());
                                     pipeline.addLast(new DummySender());
                                 }
                             }
                    );
            return b.connect("localhost", randomPort);
    }


    /**
     * A dummy class to send a unique batch to an active server
     *
     */
    private class DummySender extends SimpleChannelInboundHandler<String> {
        public void channelActive(ChannelHandlerContext ctx) {
            Batch batch = new Batch();
            batch.setBatchSize(1);
            batch.addMessage(new Message(1, Collections.singletonMap("hello", "world")));

            ctx.writeAndFlush(batch);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ctx.close();
        }
    }

    /**
     *  Used to assert the number of messages send to the server
     */
    private class SpyListener extends MessageListener {
        private AtomicInteger receivedCount;

        public SpyListener() {
            super();
            receivedCount = new AtomicInteger(0);
        }

        public void onNewMessage(ChannelHandlerContext ctx, Message message) {
            receivedCount.incrementAndGet();
        }

        public int getReceivedCount() {
            return receivedCount.get();
        }
    }
}
