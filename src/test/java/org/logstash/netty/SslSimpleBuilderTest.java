package org.logstash.netty;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for {@link SslSimpleBuilder}
 */
public class SslSimpleBuilderTest {
    @Test
    public void setProtocols() throws Exception {
        SslSimpleBuilder sslSimpleBuilder = new SslSimpleBuilder("/tmp", "mykeyfile", "mypass");
        assertArrayEquals(new String[]{"TLSv1.2"}, sslSimpleBuilder.getProtocols());
        sslSimpleBuilder.setProtocols(new String[]{"TLSv1.1"});
        assertArrayEquals(new String[]{"TLSv1.1"}, sslSimpleBuilder.getProtocols());
        sslSimpleBuilder.setProtocols(new String[]{"TLSv1.1", "TLSv1.2"});
        assertArrayEquals(new String[]{"TLSv1.1", "TLSv1.2"}, sslSimpleBuilder.getProtocols());
    }
}