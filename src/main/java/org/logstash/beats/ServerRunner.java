package org.logstash.beats;

import io.netty.channel.ChannelHandlerContext;
import org.logstash.netty.SslContextBuilder;
import org.logstash.netty.SslHandlerProvider;

public class ServerRunner {

    public static void main(String[] args) throws Exception {
        int clientInactivityTimeoutSeconds = 60;
        Server server = new Server("127.0.0.1", 3333, clientInactivityTimeoutSeconds, Runtime.getRuntime().availableProcessors());

        // enable TLS
        System.out.println("Using SSL");

        String sslCertificate = "/Users/andrea/workspace/certificates/client_from_root.crt";
        String sslKey = "/Users/andrea/workspace/certificates/client_from_root.key.pkcs8";
        String[] certificateAuthorities = new String[] { "/Users/andrea/workspace/certificates/root.crt" };

        SslContextBuilder sslBuilder = new SslContextBuilder(sslCertificate, sslKey, null)
                .setProtocols(new String[] { "TLSv1.2", "TLSv1.3" })
                .setClientAuthentication(SslContextBuilder.SslClientVerifyMode.REQUIRED, certificateAuthorities);
        SslHandlerProvider sslHandlerProvider = new SslHandlerProvider(sslBuilder.buildContext(), 10000);
        server.setSslHandlerProvider(sslHandlerProvider);

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
