package org.logstash.netty;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for {@link SslContextBuilder}
 */
public class SslContextBuilderTest {
    @Test
    public void setProtocols() throws Exception {
        SslContextBuilder sslContextBuilder = new SslContextBuilder("/tmp", "mykeyfile", "mypass");
        assertArrayEquals(new String[]{"TLSv1.2"}, sslContextBuilder.getProtocols());
        sslContextBuilder.setProtocols(new String[]{"TLSv1.1"});
        assertArrayEquals(new String[]{"TLSv1.1"}, sslContextBuilder.getProtocols());
        sslContextBuilder.setProtocols(new String[]{"TLSv1.1", "TLSv1.2"});
        assertArrayEquals(new String[]{"TLSv1.1", "TLSv1.2"}, sslContextBuilder.getProtocols());
    }
}