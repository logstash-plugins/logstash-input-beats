package org.logstash.beats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class V2BatchTest {
    public final static ObjectMapper MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());

    @Test
    public void testIsEmpty() {
        V2Batch batch = new V2Batch();
        assertTrue(batch.isEmpty());
        ByteBuf content = messageContents();
        batch.addMessage(1, content, content.readableBytes());
        assertFalse(batch.isEmpty());
    }

    @Test
    public void testSize() {
        V2Batch batch = new V2Batch();
        assertEquals(0, batch.size());
        ByteBuf content = messageContents();
        batch.addMessage(1, content, content.readableBytes());
        assertEquals(1, batch.size());
    }

    @Test
    public void TestGetProtocol() {
        assertEquals(Protocol.VERSION_2, new V2Batch().getProtocol());
    }

    @Test
    public void TestCompleteReturnTrueWhenIReceiveTheSameAmountOfEvent() {
        V2Batch batch = new V2Batch();
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        for(int i = 1; i <= numberOfEvent; i++) {
            ByteBuf content = messageContents();
            batch.addMessage(i, content, content.readableBytes());
        }

        assertTrue(batch.isComplete());
    }

    @Test
    public void testBigBatch() {
        V2Batch batch = new V2Batch();
        int size = 4096;
        assertEquals(0, batch.size());
        try {
            ByteBuf content = messageContents();
            for (int i = 0; i < size; i++) {
                batch.addMessage(i, content, content.readableBytes());
            }
            assertEquals(size, batch.size());
            int i = 0;
            for (Message message : batch) {
                assertEquals(message.getSequence(), i++);
            }
        }finally {
            batch.release();
        }
    }

    @Test
    public void testHighSequence(){
        V2Batch batch = new V2Batch();
        int numberOfEvent = 2;
        int startSequenceNumber = new SecureRandom().nextInt(10000);
        batch.setBatchSize(numberOfEvent);
        ByteBuf content = messageContents();

        for(int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(startSequenceNumber + i, content, content.readableBytes());
        }

        assertEquals(startSequenceNumber + numberOfEvent, batch.getHighestSequence());
    }


    @Test
    public void TestCompleteReturnWhenTheNumberOfEventDoesntMatchBatchSize() {
        V2Batch batch = new V2Batch();
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);
        ByteBuf content = messageContents();
        batch.addMessage(1, content, content.readableBytes());

        assertFalse(batch.isComplete());
    }

    public static ByteBuf messageContents() {
        Map test = new HashMap();
        test.put("key", "value");
        try {
            byte[] bytes = MAPPER.writeValueAsBytes(test);
            return Unpooled.wrappedBuffer(bytes);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}