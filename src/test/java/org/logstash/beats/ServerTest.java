package org.logstash.beats;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Before;
import org.junit.Test;
import java.util.concurrent.ThreadLocalRandom;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;

public class ServerTest {
    private int randomPort;
    @Before
    public void setUp() {
        randomPort = ThreadLocalRandom.current().nextInt(1024, 65535);
    }

    @Test
    public void testServerShouldTerminateConnectionIdleForTooLong() throws InterruptedException {
        int inactivityTime = 3; // in seconds

        Server server = new Server(randomPort);
        server.setClientInactivityTimeout(inactivityTime);

        Runnable serverTask = () -> {
            try {
                server.listen();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };

        Thread thread = new Thread(serverTask);
        thread.start();

        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                                 @Override
                                 public void initChannel(SocketChannel ch) throws Exception {
                                 }
                             }
                    );

            Long started = System.currentTimeMillis() / 1000L;

            ChannelFuture f = b.connect("localhost", randomPort).sync();
            f.channel().closeFuture().sync();

            Long ended = System.currentTimeMillis() / 1000L;

            int diff = (int) (ended - started);
            assertThat(diff, is(greaterThanOrEqualTo(inactivityTime)));
        } finally {
            group.shutdownGracefully();
            server.stop();
        }
    }
}
