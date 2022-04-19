package org.logstash.netty;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.crypto.Cipher;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLServerSocketFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    public static final Set<String> SUPPORTED_CIPHERS = new HashSet<>(Arrays.asList(
        ((SSLServerSocketFactory) SSLServerSocketFactory.getDefault()).getSupportedCipherSuites()
    ));

    /*
    Mordern Ciphers List from
    https://wiki.mozilla.org/Security/Server_Side_TLS
    */
    private final static String[] DEFAULT_CIPHERS;
    static {
        String[] defaultCipherCandidates = new String[] {
            // Modern compatibility
            "TLS_AES_256_GCM_SHA384", // TLS 1.3
            "TLS_AES_128_GCM_SHA256", // TLS 1.3
            "TLS_CHACHA20_POLY1305_SHA256", // TLS 1.3 (since Java 11.0.14)
            // Intermediate compatibility
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256", // (since Java 11.0.14)
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256", // (since Java 11.0.14)
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            // Backward compatibility
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256"
        };
        DEFAULT_CIPHERS = Arrays.stream(defaultCipherCandidates).filter(SUPPORTED_CIPHERS::contains).toArray(String[]::new);
    }

    /*
      Reduced set of ciphers available when JCE Unlimited Strength Jurisdiction Policy is not installed.
     */
    private final static String[] DEFAULT_CIPHERS_LIMITED = new String[] {
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256"
    };

    private String[] ciphers = DEFAULT_CIPHERS;
    private String[] protocols = new String[] { "TLSv1.2", "TLSv1.3" };
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
        for (String cipher : ciphersSuite) {
            if (SUPPORTED_CIPHERS.contains(cipher)) {
                logger.debug("{} cipher is supported", cipher);
            } else {
                if (!isUnlimitedJCEAvailable()) {
                    logger.warn("JCE Unlimited Strength Jurisdiction Policy not installed");
                }
                throw new IllegalArgumentException("Cipher `" + cipher + "` is not available");
            }
        }

        ciphers = ciphersSuite;
        return this;
    }

    public static String[] getDefaultCiphers(){
        if (isUnlimitedJCEAvailable()) {
            return DEFAULT_CIPHERS;
        } else {
            logger.warn("JCE Unlimited Strength Jurisdiction Policy not installed - max key length is 128 bits");
            return DEFAULT_CIPHERS_LIMITED;
        }
    }

    public static boolean isUnlimitedJCEAvailable(){
        try {
            return (Cipher.getMaxAllowedKeyLength("AES") > 128);
        } catch (NoSuchAlgorithmException e) {
            logger.warn("AES not available", e);
            return false;
        }
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

    public SslContext buildContext() throws Exception {
        io.netty.handler.ssl.SslContextBuilder builder = io.netty.handler.ssl.SslContextBuilder.forServer(sslCertificateFile, sslKeyFile, passPhrase);

        if (logger.isDebugEnabled()) {
            logger.debug("Available ciphers: " + SUPPORTED_CIPHERS);
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

        try {
            return builder.build();
        } catch (SSLException e) {
            logger.debug("Failed to initialize SSL", e);
            // unwrap generic wrapped exception from Netty's JdkSsl{Client|Server}Context
            if ("failed to initialize the server-side SSL context".equals(e.getMessage()) ||
                "failed to initialize the client-side SSL context".equals(e.getMessage())) {
                // Netty catches Exception and simply wraps: throw new SSLException("...", e);
                if (e.getCause() instanceof Exception) throw (Exception) e.getCause();
            }
            throw e;
        } catch (Exception e) {
            logger.debug("Failed to initialize SSL", e);
            throw e;
        }
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
