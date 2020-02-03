package org.logstash.netty;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

public class SslHandlerProvider {

    private final SslContext sslContext;
    private final int sslHandshakeTimeoutMillis;

    public SslHandlerProvider(SslContext context, int sslHandshakeTimeoutMillis){
        this.sslContext = context;
        this.sslHandshakeTimeoutMillis = sslHandshakeTimeoutMillis;
    }

    public SslHandler sslHandlerForChannel(final SocketChannel socket) {
        SslHandler handler =  sslContext.newHandler(socket.alloc());
        handler.setHandshakeTimeoutMillis(sslHandshakeTimeoutMillis);
        return handler;
    }
}
