package org.logstash.beats;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import org.apache.log4j.Logger;
import org.logstash.netty.SslSimpleBuilder;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;



public class Server {
    private final static Logger logger = Logger.getLogger(Server.class);

    private static final int DEFAULT_CLIENT_TIMEOUT_SECONDS = 15;

    private final int port;
    private final NioEventLoopGroup bossGroup;
    private final NioEventLoopGroup workGroup;
    private final String host;
    private IMessageListener messageListener = new MessageListener();
    private SslSimpleBuilder sslBuilder;

    private final int clientInactivityTimeoutSeconds;

    public Server(String host, int p) {
        this(host, p, DEFAULT_CLIENT_TIMEOUT_SECONDS);
    }

    public Server(String host, int p, int timeout) {
        this.host = host;
        port = p;
        clientInactivityTimeoutSeconds = timeout;
        bossGroup = new NioEventLoopGroup();
        workGroup = new NioEventLoopGroup();
    }

    public void enableSSL(SslSimpleBuilder builder) {
        sslBuilder = builder;
    }

    public Server listen() throws InterruptedException {
        BeatsInitializer beatsInitializer = null;

        try {
            logger.info("Starting server on port: " +  this.port);

            beatsInitializer = new BeatsInitializer(isSslEnable(), messageListener, clientInactivityTimeoutSeconds);

            ServerBootstrap server = new ServerBootstrap();
            server.group(bossGroup, workGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_LINGER, 0) // Since the protocol doesn't support yet a remote close from the server and we don't want to have 'unclosed' socket lying around we have to use `SO_LINGER` to force the close of the socket.
                    .childHandler(beatsInitializer);

            Channel channel = server.bind(host, port).sync().channel();
            channel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully().sync();
            workGroup.shutdownGracefully().sync();
            beatsInitializer.shutdownEventExecutor();
        }

        return this;
    }

    public void stop() throws InterruptedException {
        logger.debug("Server shutting down");

        bossGroup.shutdownGracefully().sync();
        workGroup.shutdownGracefully().sync();

        logger.debug("Server stopped");
    }

    public void setMessageListener(IMessageListener listener) {
        messageListener = listener;
    }

    public boolean isSslEnable() {
        return this.sslBuilder != null;
    }

    private class BeatsInitializer extends ChannelInitializer<SocketChannel> {
        private final String LOGGER_HANDLER = "logger";
        private final String SSL_HANDLER = "ssl-handler";
        private final String KEEP_ALIVE_HANDLER = "keep-alive-handler";
        private final String BEATS_PARSER = "beats-parser";
        private final String BEATS_HANDLER = "beats-handler";
        private final String BEATS_ACKER = "beats-acker";

        private final int DEFAULT_IDLESTATEHANDLER_THREAD = 4;
        private final int IDLESTATE_WRITER_IDLE_TIME_SECONDS = 5;
        private final int IDLESTATE_ALL_IDLE_TIME_SECONDS = 0;

        private final EventExecutorGroup idleExecutorGroup;
        private final IMessageListener message;
        private int clientInactivityTimeoutSeconds;
        private final LoggingHandler loggingHandler = new LoggingHandler();


        private boolean enableSSL = false;

        public BeatsInitializer(Boolean secure, IMessageListener messageListener, int clientInactivityTimeoutSeconds) {
            enableSSL = secure;
            this.message = messageListener;
            this.clientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
            idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLESTATEHANDLER_THREAD);
        }

        public void initChannel(SocketChannel socket) throws IOException, NoSuchAlgorithmException, CertificateException {
            ChannelPipeline pipeline = socket.pipeline();

            pipeline.addLast(LOGGER_HANDLER, loggingHandler);

            if(enableSSL) {
                SslHandler sslHandler = sslBuilder.build(socket.alloc());
                pipeline.addLast(SSL_HANDLER, sslHandler);
            }

            // We have set a specific executor for the idle check, because the `beatsHandler` can be
            // blocked on the queue, this the idleStateHandler manage the `KeepAlive` signal.
            pipeline.addLast(idleExecutorGroup, KEEP_ALIVE_HANDLER, new IdleStateHandler(clientInactivityTimeoutSeconds, IDLESTATE_WRITER_IDLE_TIME_SECONDS , IDLESTATE_ALL_IDLE_TIME_SECONDS));

            pipeline.addLast(BEATS_PARSER, new BeatsParser());
            pipeline.addLast(BEATS_ACKER, new AckEncoder());
            pipeline.addLast(BEATS_HANDLER, new BeatsHandler(this.message));

        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.warn("Channel initializer");
            this.message.onChannelInitializeException(ctx, cause);
        }

        public void shutdownEventExecutor() {
            try {
                idleExecutorGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                // we are shutting down we don't care about any errors here.
            }
        }
    }
}
