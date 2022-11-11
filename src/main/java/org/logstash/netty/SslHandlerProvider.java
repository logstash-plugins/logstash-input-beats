package org.logstash.netty;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.net.InetSocketAddress;

public class SslHandlerProvider {

    private final SslContext sslContext;
    private final int sslHandshakeTimeoutMillis;

    public SslHandlerProvider(SslContext context, int sslHandshakeTimeoutMillis){
        this.sslContext = context;
        this.sslHandshakeTimeoutMillis = sslHandshakeTimeoutMillis;
    }

    public SslHandler sslHandlerForChannel(final SocketChannel socketChannel) {
        final InetSocketAddress remoteAddress = socketChannel.remoteAddress();
        final String peerHost = remoteAddress.getHostString();
        final int peerPort = remoteAddress.getPort();
        final SslHandler sslHandler = sslContext.newHandler(socketChannel.alloc(), peerHost, peerPort);

        final SSLEngine engine = sslHandler.engine();
        engine.setUseClientMode(false);

        final SSLParameters sslParameters = engine.getSSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(sslParameters);

        sslHandler.setHandshakeTimeoutMillis(sslHandshakeTimeoutMillis);
        return sslHandler;
    }
}
