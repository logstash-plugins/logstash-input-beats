package org.logstash.beats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class V2BatchTest {
    public final static ObjectMapper MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());
    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();

    @Before
    public void setUp() {
        System.setProperty("log4j.configurationFile", "log4j2-v2bartch-test.properties");
        System.setOut(new PrintStream(outputStreamCaptor));
    }

    @After
    public void tearDown() {
        System.setOut(standardOut);
    }


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

    @Test
    public void givenBufferSizeThatFitIntoActualMaxOrderThenNoLogLineIsPrinted() {
        V2Batch sut = new V2Batch();

        sut.eventuallyLogIdealMaxOrder(1024 * 1024);

        final String output = outputStreamCaptor.toString();
        assertThat("No logging when the buffer size fit into actual maxOrder", output, isEmptyString());
    }

    @Test
    public void givenBufferSizeThatDoesntFitIntoActualMaxOrderThenLogLineIsPrintedJustOnce() {
        V2Batch sut = new V2Batch();
        int actualChunkSize = PooledByteBufAllocator.DEFAULT.metric().chunkSize();
        sut.eventuallyLogIdealMaxOrder(actualChunkSize + 1024);

        final String output = outputStreamCaptor.toString();
        Pattern pattern = Pattern.compile("^.*Got a batch size of \\d* bytes, while this instance expects batches up to \\d*, please bump maxOrder to \\d*.*", Pattern.DOTALL);
        assertTrue("First time the chunk size is passed a log line is printed", pattern.matcher(output).find());

        sut.eventuallyLogIdealMaxOrder(actualChunkSize + 1024);
        final String newOutput = outputStreamCaptor.toString();
        assertTrue("Second time the same chunk size is passed, no log happens", pattern.matcher(newOutput).find());
    }

    @Test
    public void givenBufferSizeBiggerThanMaximumNettyChunkSizeThenSpecificErrorLineIsLogged() {
        V2Batch sut = new V2Batch();
        int maxChunkSize = PooledByteBufAllocator.defaultPageSize() << 14;
        sut.eventuallyLogIdealMaxOrder(maxChunkSize + 1024);

        final String output = outputStreamCaptor.toString();
        Pattern pattern = Pattern.compile("^.*Got a batch size of \\d* bytes that can fit into maximum maxOrder value 14, can't increment more.*", Pattern.DOTALL);
        boolean matchResult = pattern.matcher(output).matches();
        assertTrue("Error message to be over the maximum Netty chunk size is printed, but was: |" + output + "|" , matchResult);
    }
}