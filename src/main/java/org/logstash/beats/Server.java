package org.logstash.beats;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.netty.SslHandlerProvider;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Optional;

import static org.logstash.beats.util.DaemonThreadFactory.daemonThreadFactory;

public class Server {
    private final static Logger logger = LogManager.getLogger(Server.class);

    private final int port;

    private final String id;
    private final String host;
    private final int eventLoopThreadCount;
    private final int executorThreadCount;
    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workGroup;
    private volatile Channel serverChannel;
    private SslHandlerProvider sslHandlerProvider;
    private BeatsInitializer beatsInitializer;

    private final int clientInactivityTimeoutSeconds;

    public Server(String id, String host, int port, int clientInactivityTimeoutSeconds, int eventLoopThreadCount, int executorThreadCount) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.clientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
        this.eventLoopThreadCount = eventLoopThreadCount;
        this.executorThreadCount = executorThreadCount;
    }

    public void setSslHandlerProvider(SslHandlerProvider sslHandlerProvider) {
        this.sslHandlerProvider = sslHandlerProvider;
    }

    public synchronized void bind() throws InterruptedException {
        if (workGroup != null) {
            try {
                logger.debug("Shutting down existing worker group before starting");
                workGroup.shutdownGracefully().sync();
            } catch (Exception e) {
                logger.error("Could not shut down worker group before starting", e);
            }
        }
        bossGroup = new NioEventLoopGroup(eventLoopThreadCount, daemonThreadFactory(id + "-bossGroup")); // TODO: add a config to make it adjustable, no need many threads
        workGroup = new NioEventLoopGroup(eventLoopThreadCount, daemonThreadFactory(id + "-workGroup"));
        try {
            logger.info("Starting server on port: {}", this.port);

            beatsInitializer = new BeatsInitializer(id, clientInactivityTimeoutSeconds, executorThreadCount);

            ServerBootstrap server = new ServerBootstrap();
            server.group(bossGroup, workGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.AUTO_READ, false) //
                    .childOption(ChannelOption.SO_LINGER, 0) // Since the protocol doesn't support yet a remote close from the server and we don't want to have 'unclosed' socket lying around we have to use `SO_LINGER` to force the close of the socket.
                    .childHandler(beatsInitializer);

            serverChannel = server
                    .bind(host, port)
                    .sync()
                    .channel();
        } catch (InterruptedException e) {
            shutdown();
            throw e;
        }
    }

    public void run(final IMessageListener messageListener) throws InterruptedException {
        synchronized (this) {
            if (serverChannel == null) {
                bind();
            }
        }
        try {
            beatsInitializer.setMessageListener(messageListener);
            serverChannel.config().setAutoRead(true);
            serverChannel.closeFuture().sync();
        } finally {
            shutdown();
        }
    }

    public void stop() {
        logger.debug("Server shutting down");
        shutdown();
        logger.debug("Server stopped");
    }

    private void shutdown() {
        // as much as possible we try to gracefully shut down
        // no longer accept incoming socket connections
        // with event loop group (workGroup) declares quite period with `shutdownGracefully` which queued tasks will not receive work for the best cases
        // executor group threads will be asked and waited to gracefully shutdown if they have pending tasks. This helps each individual handler process the event/exception, especially when multichannel use case.
        //  there is no guarantee that executor threads will terminate during the shutdown because of many factors such as ack processing
        //  so any pending tasks which send batches to BeatsHandler will be ignored
        try {
            if (serverChannel != null && serverChannel.isOpen()) {
                serverChannel.close().sync();
            }
            // boss group shuts down socket connections
            // shutting down bossGroup separately gives us faster channel closure
            if (bossGroup != null && !bossGroup.isShutdown()) {
                bossGroup.shutdownGracefully().sync();
            }

            if (workGroup != null && !workGroup.isShutdown()) {
                workGroup.shutdownGracefully().sync();
            }

            if (beatsInitializer != null) {
                beatsInitializer.shutdownEventExecutor();
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean isSslEnabled() {
        return this.sslHandlerProvider != null;
    }

    @Override
    public String toString() {
        final String bindInfo = Optional.ofNullable(serverChannel)
                .filter(Channel::isOpen)
                .map(Channel::localAddress)
                .map(InetSocketAddress.class::cast)
                .map(InetSocketAddress::toString)
                .orElse("unbound");

        return String.format("org.logstash.beats.Server{id='%s', host='%s', port=%d, binding=%s}", id, host, port, bindInfo);
    }

    private class BeatsInitializer extends ChannelInitializer<SocketChannel> {
        private final static String SSL_HANDLER = "ssl-handler";
        private final static String IDLESTATE_HANDLER = "idlestate-handler";
        private final static String CONNECTION_HANDLER = "connection-handler";
        private final static String BEATS_ACKER = "beats-acker";


        private final static int DEFAULT_IDLESTATEHANDLER_THREAD = 4;
        private final static int IDLESTATE_WRITER_IDLE_TIME_SECONDS = 5;

        private final EventExecutorGroup idleExecutorGroup;
        private final EventExecutorGroup beatsHandlerExecutorGroup;
        private volatile IMessageListener localMessageListener = null;
        private final int localClientInactivityTimeoutSeconds;

        BeatsInitializer(String pluginId, int clientInactivityTimeoutSeconds, int beatsHandlerThreadCount) {
            // Keeps a local copy of Server settings, so they can't be modified once it starts listening
            this.localClientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
            idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLESTATEHANDLER_THREAD,
                    daemonThreadFactory(pluginId + "-idleStateHandler"));
            beatsHandlerExecutorGroup = new DefaultEventExecutorGroup(beatsHandlerThreadCount,
                    daemonThreadFactory(pluginId + "-beatsHandler"));
        }

        void setMessageListener(IMessageListener messageListener) {
            this.localMessageListener = messageListener;
        }

        public void initChannel(SocketChannel socket) {
            Objects.requireNonNull(localMessageListener, "messageListener");
            ChannelPipeline pipeline = socket.pipeline();

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

        public void shutdownEventExecutor() {
            try {
                idleExecutorGroup.shutdownGracefully().sync();

                shutdownEventExecutorsWithPendingTasks();

                // make sure non-pending tasked executors get terminated
                beatsHandlerExecutorGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        private void shutdownEventExecutorsWithPendingTasks() {
            try {
                // DefaultEventExecutorGroup internally executes numbers of SingleThreadEventExecutor
                // try to gracefully shut down every thread if they have unacked pending batches (pending tasks)
                for (final EventExecutor eventExecutor : beatsHandlerExecutorGroup) {
                    if (eventExecutor instanceof SingleThreadEventExecutor) {
                        final SingleThreadEventExecutor singleExecutor = (SingleThreadEventExecutor) eventExecutor;
                        if (singleExecutor.pendingTasks() > 0) {
                            singleExecutor.shutdownGracefully().sync();
                        }
                    }
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
