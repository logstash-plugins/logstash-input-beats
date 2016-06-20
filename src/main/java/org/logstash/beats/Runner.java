package org.logstash.beats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.netty.PrivateKeyConverter;
import org.logstash.netty.SslSimpleBuilder;

import java.io.FileInputStream;


public class Runner {
    private static final int DEFAULT_PORT = 5044;
    public static Logger logger = LogManager.getLogger(Runner.class.getName());


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

            PrivateKeyConverter converter = new PrivateKeyConverter(noPkcs7SslKey, null);

            logger.debug("SSLCertificate: " + sslCertificate);
            logger.debug("SSLKey: " + sslKey);

//            SslSimpleBuilder sslBuilder = new SslSimpleBuilder(sslCertificate, sslKey, null)
            SslSimpleBuilder sslBuilder = new SslSimpleBuilder(new FileInputStream(sslCertificate), converter.convert(), null)
                    .setProtocols(new String[] { "TLSv1.2" })
                    .setCertificateAuthorities(certificateAuthorities);
            server.enableSSL(sslBuilder);
        }

        server.listen();
    }
}