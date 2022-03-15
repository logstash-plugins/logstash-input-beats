package org.logstash.netty;

import org.hamcrest.core.Every;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.isIn;

import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLServerSocketFactory;

/**
 * Unit test for {@link SslContextBuilder}
 */
public class SslContextBuilderTest {
    @Test
    public void setProtocols() throws Exception {
        SslContextBuilder sslContextBuilder = new SslContextBuilder("src/test/resources/encrypted-pkcs8.crt", "src/test/resources/encrypted-pkcs8.key", "1234");
        assertArrayEquals(new String[]{"TLSv1.2", "TLSv1.3"}, sslContextBuilder.getProtocols());
        sslContextBuilder.setProtocols(new String[]{"TLSv1.1"});
        assertArrayEquals(new String[]{"TLSv1.1"}, sslContextBuilder.getProtocols());
        sslContextBuilder.setProtocols(new String[]{"TLSv1.1", "TLSv1.2"});
        assertArrayEquals(new String[]{"TLSv1.1", "TLSv1.2"}, sslContextBuilder.getProtocols());
    }

    @Test
    public void getDefaultCiphers() throws Exception
    {
        String[] defaultCiphers = SslContextBuilder.getDefaultCiphers();

        assertTrue(defaultCiphers.length > 0);

        // Check that default ciphers is the subset of default ciphers of current Java version.
        SSLServerSocketFactory ssf = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        List<String> availableCiphers = Arrays.asList(ssf.getSupportedCipherSuites());
        assertThat(Arrays.asList(defaultCiphers), Every.everyItem(isIn(availableCiphers)));
    }
}
