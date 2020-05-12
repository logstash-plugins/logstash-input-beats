package org.logstash.netty;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ph on 2016-05-27.
 */
public class SslContextBuilder {


    public enum SslClientVerifyMode {
        VERIFY_PEER,
        FORCE_PEER,
    }
    private final static Logger logger = LogManager.getLogger(SslContextBuilder.class);


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

    public SslContextBuilder(String sslCertificateFilePath, String sslKeyFilePath, String pass) throws IllegalArgumentException {
        sslCertificateFile = new File(sslCertificateFilePath);
        if (!sslCertificateFile.canRead()) {
            throw new IllegalArgumentException(
                    String.format("Certificate file cannot be read. Please confirm the user running Logstash has permissions to read: %s", sslCertificateFilePath));
        }
        sslKeyFile = new File(sslKeyFilePath);
        if (!sslKeyFile.canRead()) {
            throw new IllegalArgumentException(
                    String.format("Private key file cannot be read. Please confirm the user running Logstash has permissions to read: %s", sslKeyFilePath));
        }
        passPhrase = pass;
    }

    public SslContextBuilder setProtocols(String[] protocols) {
        this.protocols = protocols;
        return this;
    }

    public SslContextBuilder setCipherSuites(String[] ciphersSuite) throws IllegalArgumentException {
        for(String cipher : ciphersSuite) {
            if(!OpenSsl.isCipherSuiteAvailable(cipher)) {
                throw new IllegalArgumentException("Cipher `" + cipher + "` is not available");
            } else {
                logger.debug("Cipher is supported: {}", cipher);
            }
        }

        ciphers = ciphersSuite;
        return this;
    }

    public SslContextBuilder setCertificateAuthorities(String[] cert) {
        certificateAuthorities = cert;
        return this;
    }

    public SslContextBuilder setVerifyMode(SslClientVerifyMode mode) {
        verifyMode = mode;
        return this;
    }

    public File getSslKeyFile() {
        return sslKeyFile;
    }

    public File getSslCertificateFile() {
        return sslCertificateFile;
    }

    public SslContext buildContext() throws IOException, CertificateException  {
        io.netty.handler.ssl.SslContextBuilder builder = io.netty.handler.ssl.SslContextBuilder.forServer(sslCertificateFile, sslKeyFile, passPhrase);

        if (logger.isDebugEnabled()) {
            logger.debug("Available ciphers: " + Arrays.toString(OpenSsl.availableOpenSslCipherSuites().toArray()));
            logger.debug("Ciphers:  " + Arrays.toString(ciphers));
        }

        builder.ciphers(Arrays.asList(ciphers));

        if(requireClientAuth()) {
            if (logger.isDebugEnabled())
                logger.debug("Certificate Authorities: " + Arrays.toString(certificateAuthorities));

            builder.trustManager(loadCertificateCollection(certificateAuthorities));
            if(verifyMode == SslClientVerifyMode.FORCE_PEER) {
                // Explicitly require a client certificate
                builder.clientAuth(ClientAuth.REQUIRE);
            } else if(verifyMode == SslClientVerifyMode.VERIFY_PEER) {
                // If the client supply a client certificate we will verify it.
                builder.clientAuth(ClientAuth.OPTIONAL);
            }
        }else{
            builder.clientAuth(ClientAuth.NONE);
        }
        builder.protocols(protocols);
        return builder.build();
    }

    private X509Certificate[] loadCertificateCollection(String[] certificates) throws IOException, CertificateException {
        logger.debug("Load certificates collection");
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");

        List<X509Certificate> collections = new ArrayList<X509Certificate>();

        for(int i = 0; i < certificates.length; i++) {
            String certificate = certificates[i];

            logger.debug("Loading certificates from file {}", certificate);

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
