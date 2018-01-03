package org.logstash.beats;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.netty.SslSimpleBuilder;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;



public class Server {
    private final static Logger logger = LogManager.getLogger(Server.class);

    private static final int DEFAULT_CLIENT_TIMEOUT_SECONDS = 15;

    private final int port;
    private final NioEventLoopGroup workGroup;
    private final String host;
    private final int beatsHeandlerThreadCount;
    private IMessageListener messageListener = new MessageListener();
    private SslSimpleBuilder sslBuilder;
    private BeatsInitializer beatsInitializer;

    private final int clientInactivityTimeoutSeconds;

    public Server(String host, int p) {
        this(host, p, DEFAULT_CLIENT_TIMEOUT_SECONDS, Runtime.getRuntime().availableProcessors() * 4);
    }

    public Server(String host, int p, int timeout, int threadCount) {
        this.host = host;
        port = p;
        clientInactivityTimeoutSeconds = timeout;
        beatsHeandlerThreadCount = threadCount;
        workGroup = new NioEventLoopGroup();
    }

    public void enableSSL(SslSimpleBuilder builder) {
        sslBuilder = builder;
    }

    public Server listen() throws InterruptedException {
        try {
            logger.info("Starting server on port: " +  this.port);

            beatsInitializer = new BeatsInitializer(isSslEnable(), messageListener, clientInactivityTimeoutSeconds, beatsHeandlerThreadCount);

            ServerBootstrap server = new ServerBootstrap();
            server.group(workGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_LINGER, 0) // Since the protocol doesn't support yet a remote close from the server and we don't want to have 'unclosed' socket lying around we have to use `SO_LINGER` to force the close of the socket.
                    .childHandler(beatsInitializer);

            Channel channel = server.bind(host, port).sync().channel();
            channel.closeFuture().sync();
        } finally {
            shutdown();
        }

        return this;
    }

    public void stop() throws InterruptedException {
        logger.debug("Server shutting down");
        shutdown();
        logger.debug("Server stopped");
    }

    private void shutdown(){
        try {
            workGroup.shutdownGracefully().sync();
            beatsInitializer.shutdownEventExecutor();
        } catch (InterruptedException e){
            throw new IllegalStateException(e);
        }
    }

    public void setMessageListener(IMessageListener listener) {
        messageListener = listener;
    }

    public boolean isSslEnable() {
        return this.sslBuilder != null;
    }

    private class BeatsInitializer extends ChannelInitializer<SocketChannel> {
        private final String SSL_HANDLER = "ssl-handler";
        private final String IDLESTATE_HANDLER = "idlestate-handler";
        private final String KEEP_ALIVE_HANDLER = "keep-alive-handler";
        private final String BEATS_PARSER = "beats-parser";
        private final String BEATS_HANDLER = "beats-handler";
        private final String BEATS_ACKER = "beats-acker";


        private final int DEFAULT_IDLESTATEHANDLER_THREAD = 4;
        private final int IDLESTATE_WRITER_IDLE_TIME_SECONDS = 5;

        private final EventExecutorGroup idleExecutorGroup;
        private final EventExecutorGroup beatsHandlerExecutorGroup;
        private final IMessageListener message;
        private int clientInactivityTimeoutSeconds;

        private boolean enableSSL = false;

        public BeatsInitializer(Boolean secure, IMessageListener messageListener, int clientInactivityTimeoutSeconds, int beatsHandlerThread) {
            enableSSL = secure;
            this.message = messageListener;
            this.clientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
            idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLESTATEHANDLER_THREAD);
            beatsHandlerExecutorGroup = new DefaultEventExecutorGroup(beatsHandlerThread);

        }

        public void initChannel(SocketChannel socket) throws IOException, NoSuchAlgorithmException, CertificateException {
            ChannelPipeline pipeline = socket.pipeline();

            if(enableSSL) {
                SslHandler sslHandler = sslBuilder.build(socket.alloc());
                pipeline.addLast(SSL_HANDLER, sslHandler);
            }
            pipeline.addLast(idleExecutorGroup, IDLESTATE_HANDLER, new IdleStateHandler(clientInactivityTimeoutSeconds, IDLESTATE_WRITER_IDLE_TIME_SECONDS , clientInactivityTimeoutSeconds));
            pipeline.addLast(BEATS_ACKER, new AckEncoder());
            pipeline.addLast(BEATS_PARSER, new BeatsParser());
            pipeline.addLast(beatsHandlerExecutorGroup, BEATS_HANDLER, new BeatsHandler(this.message));
            pipeline.addLast(KEEP_ALIVE_HANDLER, new KeepAliveHandler());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.warn("Exception caught in channel initializer", cause);
            this.message.onChannelInitializeException(ctx, cause);
        }

        public void shutdownEventExecutor() {
            try {
                idleExecutorGroup.shutdownGracefully().sync();
                beatsHandlerExecutorGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
