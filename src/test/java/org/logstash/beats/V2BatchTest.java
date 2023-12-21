package org.logstash.beats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class V2BatchTest {
    private final static Logger logger = LogManager.getLogger(V2BatchTest.class);
    public final static ObjectMapper MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());
    private SpyLogger loggerSpy;

    @Before
    public void setUp() {
        loggerSpy = new SpyLogger(logger);
    }

    @After
    public void tearDown() {
        V2Batch.resetReportedOrders();
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
        } finally {
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

        sut.eventuallyLogIdealMaxOrder(1024 * 1024, loggerSpy);

        loggerSpy.verifyNoLog("No logging when the buffer size fit into actual maxOrder");
    }

    @Test
    public void givenBufferSizeThatDoesntFitIntoActualMaxOrderThenLogLineIsPrintedJustOnce() {
        V2Batch sut = new V2Batch();
        int actualChunkSize = PooledByteBufAllocator.DEFAULT.metric().chunkSize();
        sut.eventuallyLogIdealMaxOrder(actualChunkSize + 1024, loggerSpy);

        loggerSpy.verifyLogMessage("First time the chunk size is passed a log line is printed",
                "Received batch of size {} bytes that is too large to fit into the pre-allocated memory pool. This will cause a performance degradation. Set 'io.netty.allocator.maxOrder' JVM property to {} to accommodate batches bigger than {} bytes.");
        loggerSpy.verifyLevel(Level.WARN);

        sut.eventuallyLogIdealMaxOrder(actualChunkSize + 1024, loggerSpy);
        loggerSpy.verifyNoLog("Second time the same chunk size is passed, no log happens");
    }

    @Test
    public void givenBufferSizeBiggerThanMaximumNettyChunkSizeThenSpecificErrorLineIsLogged() {
        V2Batch sut = new V2Batch();
        int maxChunkSize = PooledByteBufAllocator.defaultPageSize() << 14;
        sut.eventuallyLogIdealMaxOrder(maxChunkSize + 1024, loggerSpy);

        loggerSpy.verifyLogMessage("Error message to be over the maximum Netty chunk size is printed",
                "Received batch of size {} bytes that is too large to fit into the pre-allocated memory pool. Reduce the size of the batch to improve performance and avoid data loss.");
        loggerSpy.verifyLevel(Level.ERROR);
    }

    @Test
    public void givenWarningLogAlreadyPrintedForMaxOrderThenAnyOtherIdealMaxOrderMinorThanThatArentPrinted() {
        // actual maxOrder is 8, the system has already reported an ideal maxOrder of 11, then it wouldn't report
        // any other maxOrder in the range 9..11
        V2Batch sut = new V2Batch();
        int maxChunkSize = PooledByteBufAllocator.defaultPageSize() << 12;
        sut.eventuallyLogIdealMaxOrder(maxChunkSize, loggerSpy);

        loggerSpy.verifyLogMessage("First time the chunk size is passed a log line is printed",
                "Received batch of size {} bytes that is too large to fit into the pre-allocated memory pool. This will cause a performance degradation. Set 'io.netty.allocator.maxOrder' JVM property to {} to accommodate batches bigger than {} bytes.");
        loggerSpy.verifyLevel(Level.WARN);

        maxChunkSize = PooledByteBufAllocator.defaultPageSize() << 10;
        sut.eventuallyLogIdealMaxOrder(maxChunkSize, loggerSpy);

        loggerSpy.verifyNoLog("MaxOrder lover then the one already reported aren't logged");
    }
}