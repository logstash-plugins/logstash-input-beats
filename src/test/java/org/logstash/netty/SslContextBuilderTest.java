package org.logstash.netty;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import org.hamcrest.core.Every;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.logstash.netty.SslContextBuilder.SUPPORTED_CIPHERS;
import static org.logstash.netty.SslContextBuilder.SslClientVerifyMode;
import static org.logstash.netty.SslContextBuilder.getDefaultCiphers;

/**
 * Unit test for {@link SslContextBuilder}
 */
public class SslContextBuilderTest {

    private static final String CERTIFICATE = "src/test/resources/host.crt";
    private static final String KEY = "src/test/resources/host.key";
    private static final String KEY_ENCRYPTED = "src/test/resources/host.enc.key";
    private static final String KEY_ENCRYPTED_PASS = "1234";
    private static final String CA = "src/test/resources/root-ca.crt";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testConstructorShouldFailWhenCertificatePathIsInvalid() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Certificate file cannot be read. Please confirm the user running Logstash has permissions to read: foo-bar.crt");
        new SslContextBuilder("foo-bar.crt", KEY_ENCRYPTED, KEY_ENCRYPTED_PASS);
    }

    @Test
    public void testConstructorShouldFailWhenKeyPathIsInvalid() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Private key file cannot be read. Please confirm the user running Logstash has permissions to read: invalid.key");
        new SslContextBuilder(CERTIFICATE, "invalid.key", KEY_ENCRYPTED_PASS);
    }

    @Test
    public void testSetCipherSuitesShouldNotFailIfAllCiphersAreValid() {
        final SslContextBuilder sslContextBuilder = createSslContextBuilder();
        sslContextBuilder.setCipherSuites(SUPPORTED_CIPHERS.toArray(new String[0]));
    }

    @Test
    public void testSetCipherSuitesShouldThrowIfAnyCiphersIsInValid() {
        final SslContextBuilder sslContextBuilder = createSslContextBuilder();
        final String[] ciphers = SUPPORTED_CIPHERS
                .toArray(new String[SUPPORTED_CIPHERS.size() + 1]);

        ciphers[ciphers.length - 1] = "TLS_INVALID_CIPHER";

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Cipher `TLS_INVALID_CIPHER` is not available");

        sslContextBuilder.setCipherSuites(ciphers);
    }

    @Test
    public void testSetProtocols() {
        final SslContextBuilder sslContextBuilder = createSslContextBuilder();
        assertArrayEquals(new String[]{"TLSv1.2", "TLSv1.3"}, sslContextBuilder.getProtocols());
        sslContextBuilder.setProtocols(new String[]{"TLSv1.1"});
        assertArrayEquals(new String[]{"TLSv1.1"}, sslContextBuilder.getProtocols());
        sslContextBuilder.setProtocols(new String[]{"TLSv1.1", "TLSv1.2"});
        assertArrayEquals(new String[]{"TLSv1.1", "TLSv1.2"}, sslContextBuilder.getProtocols());
    }

    @Test
    public void testGetDefaultCiphers() {
        String[] defaultCiphers = getDefaultCiphers();

        assertTrue(defaultCiphers.length > 0);

        // Check that default ciphers is the subset of default ciphers of current Java version.
        SSLServerSocketFactory ssf = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        List<String> availableCiphers = Arrays.asList(ssf.getSupportedCipherSuites());
        assertThat(Arrays.asList(defaultCiphers), Every.everyItem(isIn(availableCiphers)));
    }

    @Test
    public void testSetClientAuthentication() {
        final SslContextBuilder sslContextBuilder = createSslContextBuilder();
        final String[] certificateAuthorities = {CA};

        sslContextBuilder.setClientAuthentication(SslClientVerifyMode.REQUIRED, certificateAuthorities);

        assertThat(sslContextBuilder.getVerifyMode(), is(SslClientVerifyMode.REQUIRED));
        assertThat(Arrays.asList(sslContextBuilder.getCertificateAuthorities()), Every.everyItem(isIn(certificateAuthorities)));
    }

    @Test
    public void testSetClientAuthenticationWithRequiredAndNoCertAuthorities() {
        assertSetClientAuthenticationThrowWithNoCerts(SslClientVerifyMode.REQUIRED);
    }

    @Test
    public void testSetClientAuthenticationWithOptionalAndNoCertAuthorities() {
        assertSetClientAuthenticationThrowWithNoCerts(SslClientVerifyMode.OPTIONAL);
    }

    @Test
    public void testSetClientAuthenticationWithNone() {
        final SslContextBuilder sslContextBuilder = createSslContextBuilder();

        sslContextBuilder.setClientAuthentication(SslClientVerifyMode.NONE, new String[0]);
        assertThat(sslContextBuilder.getVerifyMode(), is(SslClientVerifyMode.NONE));
        assertThat(sslContextBuilder.getCertificateAuthorities(), arrayWithSize(0));

        sslContextBuilder.setClientAuthentication(SslClientVerifyMode.NONE, null);
        assertThat(sslContextBuilder.getVerifyMode(), is(SslClientVerifyMode.NONE));
        assertThat(sslContextBuilder.getCertificateAuthorities(), nullValue());
    }

    @Test
    public void testDefaultVerifyMode() {
        final SslContextBuilder sslContextBuilder = createSslContextBuilder();
        assertThat(sslContextBuilder.getVerifyMode(), is(SslClientVerifyMode.NONE));
    }

    private void assertSetClientAuthenticationThrowWithNoCerts(SslClientVerifyMode mode) {
        final SslContextBuilder sslContextBuilder = createSslContextBuilder();
        final String expectedMessage = "Certificate authorities are required to enable client authentication";

        try {
            sslContextBuilder.setClientAuthentication(mode, new String[0]);
            fail("No exception was thrown");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(expectedMessage));
        }

        try {
            sslContextBuilder.setClientAuthentication(mode, null);
            fail("No exception was thrown");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(expectedMessage));
        }
    }

    @Test
    public void testSslClientVerifyModeToClientAuth() {
        assertThat(SslClientVerifyMode.REQUIRED.toClientAuth(), is(ClientAuth.REQUIRE));
        assertThat(SslClientVerifyMode.OPTIONAL.toClientAuth(), is(ClientAuth.OPTIONAL));
        assertThat(SslClientVerifyMode.NONE.toClientAuth(), is(ClientAuth.NONE));
    }

    @Test
    public void testBuildContextWithNonEncryptedKey() throws Exception {
        SslContextBuilder sslContextBuilder = new SslContextBuilder(CERTIFICATE, KEY, null);
        sslContextBuilder.buildContext();
    }

    @Test
    public void testBuildContextWithEncryptedKey() throws Exception {
        SslContextBuilder sslContextBuilder = new SslContextBuilder(CERTIFICATE, KEY_ENCRYPTED, "1234");
        sslContextBuilder.buildContext();
    }

    @Test
    public void testBuildContextWithClientAuthentication() throws Exception {
        assertSslContextBuilderBuildContext(createSslContextBuilder()
                .setClientAuthentication(SslClientVerifyMode.REQUIRED, new String[]{CA})
        );

        assertSslContextBuilderBuildContext(createSslContextBuilder()
                .setClientAuthentication(SslClientVerifyMode.OPTIONAL, new String[]{CA})
        );
    }

    @Test
    public void testBuildContextWithNoClientAuthentication() throws Exception {
        SslContextBuilder sslContextBuilder = createSslContextBuilder()
                .setClientAuthentication(SslClientVerifyMode.NONE, new String[]{CA});

        assertSslContextBuilderBuildContext(sslContextBuilder);
    }

    @Test
    public void testIsClientAuthenticationRequired() {
        final SslContextBuilder sslContextBuilder = createSslContextBuilder();
        final String[] certificateAuthorities = {CA};

        sslContextBuilder.setClientAuthentication(SslClientVerifyMode.NONE, certificateAuthorities);
        assertFalse(sslContextBuilder.isClientAuthenticationRequired());

        sslContextBuilder.setClientAuthentication(SslClientVerifyMode.OPTIONAL, certificateAuthorities);
        assertFalse(sslContextBuilder.isClientAuthenticationRequired());

        sslContextBuilder.setClientAuthentication(SslClientVerifyMode.REQUIRED, certificateAuthorities);
        assertTrue(sslContextBuilder.isClientAuthenticationRequired());
    }

    private void assertSslContextBuilderBuildContext(SslContextBuilder sslContextBuilder) throws Exception {
        final SslContext context = sslContextBuilder.buildContext();

        assertTrue(context.isServer());

        final SSLEngine sslEngine = context.newEngine(ByteBufAllocator.DEFAULT);
        assertThat(sslEngine.getEnabledCipherSuites(), equalTo(sslContextBuilder.getCiphers()));
        assertThat(sslEngine.getEnabledProtocols(), equalTo(sslContextBuilder.getProtocols()));

        if (sslContextBuilder.getVerifyMode() == SslClientVerifyMode.NONE) {
            assertFalse(sslEngine.getNeedClientAuth());
            assertFalse(sslEngine.getWantClientAuth());
        } else if (sslContextBuilder.getVerifyMode() == SslClientVerifyMode.OPTIONAL) {
            assertFalse(sslEngine.getNeedClientAuth());
            assertTrue(sslEngine.getWantClientAuth());
        } else {
            assertTrue(sslEngine.getNeedClientAuth());
            assertFalse(sslEngine.getWantClientAuth());
        }
    }

    private SslContextBuilder createSslContextBuilder() {
        return new SslContextBuilder(CERTIFICATE, KEY_ENCRYPTED, KEY_ENCRYPTED_PASS);
    }
}
