package org.logstash.beats;

import org.junit.Test;
import org.logstash.beats.Protocol;

import static org.junit.Assert.*;

/**
 * Created by ph on 2016-06-01.
 */
public class ProtocolTest {
    @Test
    public void isVersion2Test() {
        assertTrue(Protocol.isVersion2((byte) '2'));
        assertFalse(Protocol.isVersion2((byte) '1'));
    }
}
