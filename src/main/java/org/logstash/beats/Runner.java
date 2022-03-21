package org.logstash.beats;

import io.netty.handler.ssl.SslContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.netty.SslContextBuilder;
import org.logstash.netty.SslHandlerProvider;


public class Runner {
    private static final int DEFAULT_PORT = 5044;

    private final static Logger logger = LogManager.getLogger(Runner.class);



    static public void main(String[] args) throws Exception {
        logger.info("Starting Beats Bulk");

        // Check for leaks.
        // ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        Server server = new Server("0.0.0.0", DEFAULT_PORT, 15, Runtime.getRuntime().availableProcessors());

            if(args.length > 0 && args[0].equals("ssl")) {
            logger.debug("Using SSL");

            String sslCertificate = "/Users/ph/es/certificates/certificate.crt";
            String sslKey = "/Users/ph/es/certificates/certificate.pkcs8.key";
            String noPkcs7SslKey = "/Users/ph/es/certificates/certificate.key";
            String[] certificateAuthorities = new String[] { "/Users/ph/es/certificates/certificate.crt" };



            SslContextBuilder sslBuilder = new SslContextBuilder(sslCertificate, sslKey, null)
                    .setProtocols(new String[] { "TLSv1.2", "TLSv1.3" })
                    .setCertificateAuthorities(certificateAuthorities);
            SslHandlerProvider sslHandlerProvider = new SslHandlerProvider(sslBuilder.buildContext(), 10000);
            server.setSslHandlerProvider(sslHandlerProvider);
        }

        server.listen();
    }
}
