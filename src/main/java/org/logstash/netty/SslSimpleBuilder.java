package org.logstash.netty;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
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
    private String certificateAuthorities;
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

    public SslSimpleBuilder setCipherSuites(String [] ciphersSuite) {
        ciphers = ciphersSuite;
        return this;
    }

    public SslSimpleBuilder setCertificateAuthorities(String cert) {
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

    public SslHandler build(ByteBufAllocator bufferAllocator) throws SSLException, NoSuchAlgorithmException {
        SslContextBuilder builder = SslContextBuilder.forServer(sslCertificateFile, sslKeyFile, passPhrase);
        logger.debug("Ciphers: " + String.join(",", ciphers));

        builder.ciphers(Arrays.asList(ciphers));

        if(requireClientAuth()) {
            logger.debug("Certificate Authorities: " + certificateAuthorities);
            builder.trustManager(new File(certificateAuthorities));
        }

        SslContext context = builder.build();
        SslHandler sslHandler = context.newHandler(bufferAllocator);

        logger.debug("TLS: " +  String.join(",", protocols));

        SSLEngine engine = sslHandler.engine();
        logger.debug("Enabled protocols:" + String.join(", ", engine.getEnabledProtocols()));
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