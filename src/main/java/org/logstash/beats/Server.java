package org.logstash.beats;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.ChannelMatchers;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.netty.SslHandlerProvider;

import java.util.concurrent.TimeUnit;

public class Server {
    private final static Logger logger = LogManager.getLogger(Server.class);

    private final int port;
    private final String host;
    private final int eventLoopThreadCount;
    private final int executorThreadCount;
    private NioEventLoopGroup workGroup;
    private IMessageListener messageListener = new MessageListener();
    private SslHandlerProvider sslHandlerProvider;
    private BeatsInitializer beatsInitializer;

    private final int clientInactivityTimeoutSeconds;

    public Server(String host, int port, int clientInactivityTimeoutSeconds, int eventLoopThreadCount, int executorThreadCount) {
        this.host = host;
        this.port = port;
        this.clientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
        this.eventLoopThreadCount = eventLoopThreadCount;
        this.executorThreadCount = executorThreadCount;
    }

    public void setSslHandlerProvider(SslHandlerProvider sslHandlerProvider) {
        this.sslHandlerProvider = sslHandlerProvider;
    }

    public Server listen() throws InterruptedException {
        if (workGroup != null) {
            try {
                logger.debug("Shutting down existing worker group before starting");
                workGroup.shutdownGracefully().sync();
            } catch (Exception e) {
                logger.error("Could not shut down worker group before starting", e);
            }
        }
        workGroup = new NioEventLoopGroup(eventLoopThreadCount);
        try {
            logger.info("Starting server on port: {}", this.port);

            beatsInitializer = new BeatsInitializer(messageListener, clientInactivityTimeoutSeconds, executorThreadCount);

            ServerBootstrap server = new ServerBootstrap();
            server.group(workGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_LINGER, 0) // Since the protocol doesn't support yet a remote close from the server and we don't want to have 'unclosed' socket lying around we have to use `SO_LINGER` to force the close of the socket.
                    .childHandler(beatsInitializer);

            Channel channel = server
                    .bind(host, port)
                    .sync()
                    .channel();
            channel.closeFuture()
                    .sync();
        } finally {
            shutdown();
        }

        return this;
    }

    public void stop() {
        logger.debug("Server shutting down");
        shutdown();
        logger.debug("Server stopped");
    }

    private void shutdown() {
        try {
            if (beatsInitializer != null) {
                beatsInitializer.shutdownConnectionsAndExecutors();
            }
            if (workGroup != null) {
                logger.debug("Shutting down worker group");
                workGroup.shutdownGracefully().sync();
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @note used in tests
     * @return the message listener
     */
    public IMessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(IMessageListener listener) {
        messageListener = listener;
    }

    public boolean isSslEnabled() {
        return this.sslHandlerProvider != null;
    }

    private class BeatsInitializer extends ChannelInitializer<SocketChannel> {
        private final static String SSL_HANDLER = "ssl-handler";
        private final static String IDLESTATE_HANDLER = "idlestate-handler";
        private final static String CONNECTION_HANDLER = "connection-handler";
        private final static String BEATS_ACKER = "beats-acker";


        private final static int DEFAULT_IDLESTATEHANDLER_THREAD = 4;
        private final static int IDLESTATE_WRITER_IDLE_TIME_SECONDS = 5;

        private final ChannelGroup activeChannelGroup;
        private final EventExecutorGroup idleExecutorGroup;
        private final EventExecutorGroup beatsHandlerExecutorGroup;
        private final IMessageListener localMessageListener;
        private final int localClientInactivityTimeoutSeconds;

        BeatsInitializer(IMessageListener messageListener, int clientInactivityTimeoutSeconds, int beatsHandlerThread) {
            // Keeps a local copy of Server settings, so they can't be modified once it starts listening
            this.localMessageListener = messageListener;
            this.localClientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
            idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLESTATEHANDLER_THREAD);
            beatsHandlerExecutorGroup = new DefaultEventExecutorGroup(beatsHandlerThread);
            activeChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        }

        public void initChannel(SocketChannel socket) {
            activeChannelGroup.add(socket);
            ChannelPipeline pipeline = socket.pipeline();

            pipeline.addLast(new ChannelInboundHandlerAdapter(){
                @Override
                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                    activeChannelGroup.add(ctx.channel());
                    super.channelActive(ctx);
                }

                @Override
                public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
                    activeChannelGroup.remove(ctx.channel());
                    super.channelUnregistered(ctx);
                }
            });

            if (isSslEnabled()) {
                pipeline.addLast(SSL_HANDLER, sslHandlerProvider.sslHandlerForChannel(socket));
            }
            pipeline.addLast(idleExecutorGroup, IDLESTATE_HANDLER,
                    new IdleStateHandler(localClientInactivityTimeoutSeconds, IDLESTATE_WRITER_IDLE_TIME_SECONDS, localClientInactivityTimeoutSeconds));
            pipeline.addLast(BEATS_ACKER, new AckEncoder());
            pipeline.addLast(CONNECTION_HANDLER, new ConnectionHandler());
            pipeline.addLast(beatsHandlerExecutorGroup, new BeatsParser(), new BeatsHandler(localMessageListener));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.warn("Exception caught in channel initializer", cause);
            try {
                localMessageListener.onChannelInitializeException(ctx, cause);
            } finally {
                super.exceptionCaught(ctx, cause);
            }
        }

        public void shutdownConnectionsAndExecutors() {
            try {
                logger.debug("Shutting down server channel");
                activeChannelGroup.close(ChannelMatchers.isServerChannel()).sync();

                ChannelGroupFuture channelsClosedFuture = activeChannelGroup.newCloseFuture(ChannelMatchers.isNonServerChannel());
                if (!channelsClosedFuture.isDone()) {
                    // if we have inbound connections still, give them a brief
                    // moment to finish what they have already begun before closing them
                    if (!channelsClosedFuture.await(3, TimeUnit.SECONDS)) {
                        logger.debug("Forcibly closing client connections");
                        activeChannelGroup.close().sync();
                    }
                }

                logger.debug("Shutting down executors");
                idleExecutorGroup.shutdownGracefully().sync();
                beatsHandlerExecutorGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
