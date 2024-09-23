package org.logstash.beats;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.netty.SslHandlerProvider;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class Server {
    private final static Logger logger = LogManager.getLogger(Server.class);

    private final int port;
    private final String host;
    private final int eventLoopThreadCount;
    private final int executorThreadCount;
    private NioEventLoopGroup bossGroup;
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
        bossGroup = new NioEventLoopGroup(eventLoopThreadCount); // TODO: add a config to make it adjustable, no need many threads
        workGroup = new NioEventLoopGroup(eventLoopThreadCount);
        try {
            logger.info("Starting server on port: {}", this.port);

            //beatsInitializer = new BeatsInitializer(messageListener, clientInactivityTimeoutSeconds, executorThreadCount);

            ServerBootstrap server = new ServerBootstrap();
            server.group(bossGroup, workGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_LINGER, 0) // Since the protocol doesn't support yet a remote close from the server and we don't want to have 'unclosed' socket lying around we have to use `SO_LINGER` to force the close of the socket.
                    .childHandler(new ProtocolDetectionHandler());

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
        // as much as possible we try to gracefully shut down
        // no longer accept incoming socket connections
        // with event loop group (workGroup) declares quite period with `shutdownGracefully` which queued tasks will not receive work for the best cases
        // executor group threads will be asked and waited to gracefully shutdown if they have pending tasks. This helps each individual handler process the event/exception, especially when multichannel use case.
        //  there is no guarantee that executor threads will terminate during the shutdown because of many factors such as ack processing
        //  so any pending tasks which send batches to BeatsHandler will be ignored
        try {
            // boss group shuts down socket connections
            // shutting down bossGroup separately gives us faster channel closure
            if (bossGroup != null) {
                bossGroup.shutdownGracefully().sync();
            }

            if (workGroup != null) {
                workGroup.shutdownGracefully().sync();
            }

            if (beatsInitializer != null) {
                beatsInitializer.shutdownEventExecutor();
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

        private final EventExecutorGroup idleExecutorGroup;
        private final EventExecutorGroup beatsHandlerExecutorGroup;
        private final IMessageListener localMessageListener;
        private final int localClientInactivityTimeoutSeconds;
        private BeatsHandler beatsHandler;

        BeatsInitializer(IMessageListener messageListener, int clientInactivityTimeoutSeconds, int beatsHandlerThread) {
            // Keeps a local copy of Server settings, so they can't be modified once it starts listening
            this.localMessageListener = messageListener;
            this.localClientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
            idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLESTATEHANDLER_THREAD);
            beatsHandlerExecutorGroup = new DefaultEventExecutorGroup(beatsHandlerThread);

        }

        public void addPipelineHandlers(ChannelPipeline pipeline) {
//            if (isSslEnabled()) {
//                pipeline.addLast(SSL_HANDLER, sslHandlerProvider.sslHandlerForChannel(socket));
//            }
            pipeline.addLast(idleExecutorGroup, IDLESTATE_HANDLER,
                    new IdleStateHandler(localClientInactivityTimeoutSeconds, IDLESTATE_WRITER_IDLE_TIME_SECONDS, localClientInactivityTimeoutSeconds));
            pipeline.addLast(BEATS_ACKER, new AckEncoder());
            pipeline.addLast(CONNECTION_HANDLER, new ConnectionHandler());

            this.beatsHandler = new BeatsHandler(localMessageListener);
            pipeline.addLast(beatsHandlerExecutorGroup, new BeatsParser(), beatsHandler);
        }

        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            beatsHandler.channelActive(ctx);
        }

        public void initChannel(SocketChannel socket) {
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

    @ChannelHandler.Sharable
    private class ProtocolDetectionHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf in = (ByteBuf) msg;

            if (in.readableBytes() < 4)
                return;

            in.markReaderIndex();
            byte[] magicBytes = new byte[4];
            in.readBytes(magicBytes);

            final byte[] needAcks = "GET ".getBytes(StandardCharsets.UTF_8);
            System.out.println("Required 4 bytes of 'GET ', bytes:" + Arrays.toString(needAcks));
            System.out.println("Read " + magicBytes.length + " bytes, magic:" + Arrays.toString(magicBytes));

            ChannelPipeline pipeline = ctx.pipeline();
            if (Arrays.equals(needAcks, magicBytes)) {
                System.out.println("Initializing HTTP echo server...");
                pipeline.addLast(new HttpServerCodec());
                pipeline.addLast(new HttpObjectAggregator(1048576));
                pipeline.addLast(new EchoServerHandler());
                pipeline.remove(this);
            } else {
                beatsInitializer = new BeatsInitializer(messageListener, clientInactivityTimeoutSeconds, executorThreadCount);
                beatsInitializer.addPipelineHandlers(pipeline);
                beatsInitializer.channelActive(ctx);
                pipeline.remove(this);
            }

            // TODO: is default behaviour we want to be Lumberjack (considering backwards compatibility),
            //  if not then -> ctx.close();
            super.channelRead(ctx, msg);
        }
    }

    @ChannelHandler.Sharable
    private class EchoServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
            //The close is important here in an HTTP request as it sets the Content-Length of a
            //response body back to the client.
            ctx.close();
        }
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, msg.content().copy());
            ctx.write(response);
        }
    }
}
