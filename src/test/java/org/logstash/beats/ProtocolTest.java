package org.logstash.beats;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
/**
 * Created by ph on 2016-06-01.
 */
public class ProtocolTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void validVersionTest() throws InvalidFrameProtocolException{
        assertEquals(1, Protocol.version((byte) '1'));
        assertEquals(2, Protocol.version((byte) '2'));
    }

    @Test
    public void invalidVersionTest() throws InvalidFrameProtocolException{
        thrown.expect(isA(InvalidFrameProtocolException.class));
        Protocol.version((byte) '3');
    }
}
