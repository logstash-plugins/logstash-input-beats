package org.logstash.netty;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;

/**
 * Created by ph on 2016-05-27.
 */
public class SslSimpleBuilder {
    public static enum SslClientVerifyMode {
        VERIFY_PEER,
        FORCE_PEER,
    }
    public static Logger logger = LogManager.getLogger(SslSimpleBuilder.class.getName());


    private InputStream sslKeyFile;
    private InputStream sslCertificateFile;
    private SslClientVerifyMode verifyMode = SslClientVerifyMode.FORCE_PEER;

    /*
    Mordern Ciphers List from
    https://wiki.mozilla.org/Security/Server_Side_TLS
    This list require the OpenSSl engine for netty.
    */
    public final static String[] DEFAULT_CIPHERS = new String[] {
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA38",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256"
    };

    private String[] ciphers;
    private String[] protocols = new String[] { "TLSv1.2" };
    private String[] certificateAuthorities;
    private String passPhrase;

    public SslSimpleBuilder(String sslCertificateFilePath, String sslKeyFilePath, String pass) throws FileNotFoundException {
        sslCertificateFile = createFileInputStream(sslCertificateFilePath);
        sslKeyFile = createFileInputStream(sslKeyFilePath);
        passPhrase = pass;
        ciphers = DEFAULT_CIPHERS;
    }

    public SslSimpleBuilder(InputStream sslCertificateFilePath, InputStream sslKeyFilePath, String pass) throws FileNotFoundException {
        sslCertificateFile = sslCertificateFilePath;
        sslKeyFile = sslKeyFilePath;
        passPhrase = pass;
    }

    public SslSimpleBuilder setProtocols(String[] protocols) {
        protocols = protocols;
        return this;
    }

    public SslSimpleBuilder setCipherSuites(String[] ciphersSuite) {
        ciphers = ciphersSuite;
        return this;
    }

    public SslSimpleBuilder setCertificateAuthorities(String[] cert) {
        certificateAuthorities = cert;
        return this;
    }

    public SslSimpleBuilder setVerifyMode(SslClientVerifyMode mode) {
        verifyMode = mode;
        return this;
    }

    public InputStream getSslKeyFile() {
        return sslKeyFile;
    }

    public InputStream getSslCertificateFile() {
        return sslCertificateFile;
    }

    public SslHandler build(ByteBufAllocator bufferAllocator) throws IOException, NoSuchAlgorithmException, CertificateException {
        SslContextBuilder builder = SslContextBuilder.forServer(sslCertificateFile, sslKeyFile, passPhrase);

        logger.debug("Ciphers: " + String.join(",", ciphers));
        builder.ciphers(Arrays.asList(ciphers));

        if(requireClientAuth()) {
            logger.debug("Certificate Authorities: " + String.join(", ", certificateAuthorities));
            builder.trustManager(loadCertificateCollection(certificateAuthorities));
        }

        SslContext context = builder.build();
        SslHandler sslHandler = context.newHandler(bufferAllocator);

        logger.debug("TLS: " +  String.join(",", protocols));

        SSLEngine engine = sslHandler.engine();
        engine.setEnabledProtocols(protocols);


        if(requireClientAuth()) {
            // server is doing the handshake
            engine.setUseClientMode(false);

            if(verifyMode == SslClientVerifyMode.FORCE_PEER) {
                // Explicitely require a client certificate
                engine.setNeedClientAuth(true);
            } else if(verifyMode == SslClientVerifyMode.VERIFY_PEER) {
                // If the client supply a client certificate we will verify it.
                engine.setWantClientAuth(true);
            }
        }

        return sslHandler;
    }

    private X509Certificate[] loadCertificateCollection(String[] certificates) throws IOException, CertificateException {
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");

        X509Certificate[] collections = new X509Certificate[certificates.length];

        for(int i = 0; i < certificates.length; i++) {
            String certificate = certificates[i];

            InputStream in = null;

            try {
                in = new FileInputStream(certificate);
                collections[i] = (X509Certificate) certificateFactory.generateCertificate(in);
            } finally {
                if(in != null) {
                    in.close();
                }
            }
        }

        return collections;
    }

    private boolean requireClientAuth() {
        if(certificateAuthorities != null) {
            return true;
        }

        return false;
    }

    private FileInputStream createFileInputStream(String filepath) throws FileNotFoundException {
        return new FileInputStream(filepath);
    }
}