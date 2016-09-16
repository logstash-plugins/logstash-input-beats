package org.logstash.beats;

import org.apache.log4j.Logger;
import org.logstash.netty.SslSimpleBuilder;


public class Runner {
    private static final int DEFAULT_PORT = 5044;
    private final static Logger logger = Logger.getLogger(Runner.class);



    static public void main(String[] args) throws Exception {
        logger.info("Starting Beats Bulk");

        // Check for leaks.
        // ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        Server server = new Server(DEFAULT_PORT);

            if(args.length > 0 && args[0].equals("ssl")) {
            logger.debug("Using SSL");

            String sslCertificate = "/Users/ph/es/certificates/certificate.crt";
            String sslKey = "/Users/ph/es/certificates/certificate.pkcs8.key";
            String noPkcs7SslKey = "/Users/ph/es/certificates/certificate.key";
            String[] certificateAuthorities = new String[] { "/Users/ph/es/certificates/certificate.crt" };



            SslSimpleBuilder sslBuilder = new SslSimpleBuilder(sslCertificate, sslKey, null)
                    .setProtocols(new String[] { "TLSv1.2" })
                    .setCertificateAuthorities(certificateAuthorities)
                    .setHandshakeTimeoutMilliseconds(10000);

            server.enableSSL(sslBuilder);
        }

        server.listen();
    }
}