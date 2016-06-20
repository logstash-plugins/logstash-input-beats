package org.logstash.beats;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class BatchTest {

    @Test
    public void testIsEmpty() {
        Batch batch = new Batch();
        assertTrue(batch.isEmpty());
        batch.addMessage(new Message(1, new HashMap()));
        assertFalse(batch.isEmpty());
    }

    @Test
    public void testSize() {
        Batch batch = new Batch();
        assertEquals(0, batch.size());
        batch.addMessage(new Message(1, new HashMap()));
        assertEquals(1, batch.size());
    }

    @Test
    public void TestGetDefaultProtocol() {
        Batch batch = new Batch();
        assertEquals(Protocol.VERSION_2, batch.getProtocol());
    }

    @Test
    public void TestGetSetProtocol() {
        Batch batch = new Batch();
        assertEquals(Protocol.VERSION_2, batch.getProtocol());
    }
}