package org.logstash.beats;

import io.netty.channel.ChannelHandlerContext;

public class ServerRunner {

    public static void main(String[] args) throws InterruptedException {
        int clientInactivityTimeoutSeconds = 60;
        Server server = new Server("127.0.0.1", 3334, clientInactivityTimeoutSeconds, Runtime.getRuntime().availableProcessors());

        // no TLS

        server.setMessageListener(new IMessageListener() {
            @Override
            public void onNewMessage(ChannelHandlerContext ctx, Message message) {

            }

            @Override
            public void onNewConnection(ChannelHandlerContext ctx) {

            }

            @Override
            public void onConnectionClose(ChannelHandlerContext ctx) {

            }

            @Override
            public void onException(ChannelHandlerContext ctx, Throwable cause) {

            }

            @Override
            public void onChannelInitializeException(ChannelHandlerContext ctx, Throwable cause) {

            }
        });
        // blocking till the end
        server.listen();
    }
}
