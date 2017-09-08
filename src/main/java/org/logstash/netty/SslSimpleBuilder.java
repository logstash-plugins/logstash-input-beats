package org.logstash.netty;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.beats.Server;

import javax.net.ssl.SSLEngine;
import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by ph on 2016-05-27.
 */
public class SslSimpleBuilder {


    public static enum SslClientVerifyMode {
        VERIFY_PEER,
        FORCE_PEER,
    }
    private final static Logger logger = LogManager.getLogger(SslSimpleBuilder.class);


    private File sslKeyFile;
    private File sslCertificateFile;
    private SslClientVerifyMode verifyMode = SslClientVerifyMode.FORCE_PEER;

    private long handshakeTimeoutMilliseconds = 10000;

    /*
    Mordern Ciphers List from
    https://wiki.mozilla.org/Security/Server_Side_TLS
    This list require the OpenSSl engine for netty.
    */
    public final static String[] DEFAULT_CIPHERS = new String[] {
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256"
    };

    private String[] ciphers = DEFAULT_CIPHERS;
    private String[] protocols = new String[] { "TLSv1.2" };
    private String[] certificateAuthorities;
    private String passPhrase;

    public SslSimpleBuilder(String sslCertificateFilePath, String sslKeyFilePath, String pass) throws FileNotFoundException {
        sslCertificateFile = new File(sslCertificateFilePath);
        sslKeyFile = new File(sslKeyFilePath);
        passPhrase = pass;
        ciphers = DEFAULT_CIPHERS;
    }

    public SslSimpleBuilder setProtocols(String[] protocols) {
        this.protocols = protocols;
        return this;
    }

    public SslSimpleBuilder setCipherSuites(String[] ciphersSuite) throws IllegalArgumentException {
        for(String cipher : ciphersSuite) {
            if(!OpenSsl.isCipherSuiteAvailable(cipher)) {
                throw new IllegalArgumentException("Cipher `" + cipher + "` is not available");
            } else {
                logger.debug("Cipher is supported: " + cipher);
            }
        }

        ciphers = ciphersSuite;
        return this;
    }

    public SslSimpleBuilder setCertificateAuthorities(String[] cert) {
        certificateAuthorities = cert;
        return this;
    }

    public SslSimpleBuilder setHandshakeTimeoutMilliseconds(int timeout) {
        handshakeTimeoutMilliseconds = timeout;
        return this;
    }

    public SslSimpleBuilder setVerifyMode(SslClientVerifyMode mode) {
        verifyMode = mode;
        return this;
    }

    public File getSslKeyFile() {
        return sslKeyFile;
    }

    public File getSslCertificateFile() {
        return sslCertificateFile;
    }

    public SslHandler build(ByteBufAllocator bufferAllocator) throws IOException, NoSuchAlgorithmException, CertificateException {
        SslContextBuilder builder = SslContextBuilder.forServer(sslCertificateFile, sslKeyFile, passPhrase);

        if(logger.isDebugEnabled())
            logger.debug("Available ciphers:" + Arrays.toString(OpenSsl.availableOpenSslCipherSuites().toArray()));
            logger.debug("Ciphers:  " + Arrays.toString(ciphers));


        builder.ciphers(Arrays.asList(ciphers));

        if(requireClientAuth()) {
            if (logger.isDebugEnabled())
                logger.debug("Certificate Authorities: " + Arrays.toString(certificateAuthorities));

            builder.trustManager(loadCertificateCollection(certificateAuthorities));
        }

        SslContext context = builder.build();
        SslHandler sslHandler = context.newHandler(bufferAllocator);

        if(logger.isDebugEnabled())
            logger.debug("TLS: " + Arrays.toString(protocols));

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

        sslHandler.setHandshakeTimeoutMillis(handshakeTimeoutMilliseconds);

        return sslHandler;
    }

    private X509Certificate[] loadCertificateCollection(String[] certificates) throws IOException, CertificateException {
        logger.debug("Load certificates collection");
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");

        List<X509Certificate> collections = new ArrayList<X509Certificate>();

        for(int i = 0; i < certificates.length; i++) {
            String certificate = certificates[i];

            logger.debug("Loading certificates from file " + certificate);

            try(InputStream in = new FileInputStream(certificate)) {
                List<X509Certificate> certificatesChains = (List<X509Certificate>) certificateFactory.generateCertificates(in);
                collections.addAll(certificatesChains);
            }
        }
        return collections.toArray(new X509Certificate[collections.size()]);
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

    /**
     * Get the supported protocols
     * @return a defensive copy of the supported protocols
     */
    String[] getProtocols() {
        return protocols.clone();
    }
}
