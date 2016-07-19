package org.logstash.beats;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class BatchTest {
    private Batch batch;

    @Before
    public void setUp() {
        batch = new Batch();
    }

    @Test
    public void testIsEmpty() {
        assertTrue(batch.isEmpty());
        batch.addMessage(new Message(1, new HashMap()));
        assertFalse(batch.isEmpty());
    }

    @Test
    public void testSize() {
        assertEquals(0, batch.size());
        batch.addMessage(new Message(1, new HashMap()));
        assertEquals(1, batch.size());
    }

    @Test
    public void TestGetDefaultProtocol() {
        assertEquals(Protocol.VERSION_2, batch.getProtocol());
    }

    @Test
    public void TestGetSetProtocol() {
        batch.setProtocol(Protocol.VERSION_1);
        assertEquals(Protocol.VERSION_1, batch.getProtocol());
    }

    @Test
    public void TestCompleteReturnTrueWhenIReceiveTheSameAmountOfEvent() {
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        for(int i = 0; i < numberOfEvent; i++) {
            batch.addMessage(new Message(i + 1, new HashMap()));
        }

        assertTrue(batch.complete());
    }

    @Test
    public void TestCompleteReturnWhenTheNumberOfEventDoesntMatchBatchSize() {
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        batch.addMessage(new Message(1, new HashMap()));

        assertFalse(batch.complete());
    }
}