package org.logstash.beats;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class BeatsParserTest {
    private Batch batch;
    private final int numberOfMessage = 20;

    @Rule
    public ExpectedException thrown = ExpectedException.none();


    @Before
    public void setup() {
        this.batch = new Batch();
        this.batch.setProtocol(Protocol.VERSION_2);

        for(int i = 0; i < numberOfMessage; i++) {
            Map map = new HashMap<String, String>();
            map.put("line", "Another world");
            map.put("from", "Little big Adventure");

            Message message = new Message(i + 1, map);
            this.batch.addMessage(message);
        }
    }

    @Test
    public void testEncodingDecodingJson() {
        Batch decodedBatch = decodeBatch();
        assertMessages(this.batch, decodedBatch);
    }

    @Test
    public void testCompressedEncodingDecodingJson() {
        Batch decodedBatch = decodeCompressedBatch();
        assertMessages(this.batch, decodedBatch);
    }

    @Test
    public void testEncodingDecodingFields() {
        this.batch.setProtocol(Protocol.VERSION_1);
        Batch decodedBatch = decodeBatch();
        assertMessages(this.batch, decodedBatch);
    }

    @Test
    public void testCompressedEncodingDecodingFields() {
        this.batch.setProtocol(Protocol.VERSION_1);
        Batch decodedBatch = decodeCompressedBatch();
        assertMessages(this.batch, decodedBatch);
    }

    @Test
    public void testShouldNotCrashOnGarbageData() {
        byte[] n = new byte[10000];
        new Random().nextBytes(n);
        ByteBuf randomBufferData = Unpooled.wrappedBuffer(n);

        thrown.expectCause(isA(BeatsParser.InvalidFrameProtocol.class));
        EmbeddedChannel channel = new EmbeddedChannel(new BeatsParser());
        channel.writeOutbound(randomBufferData);
        Object o = channel.readOutbound();
        channel.writeInbound(o);
    }

    private void assertMessages(Batch expected, Batch actual) {
        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());

        for(int i=0; i < expected.size(); i++) {
            assertEquals(expected.getMessages().get(i).getSequence(), actual.getMessages().get(i).getSequence());
            assertEquals(expected.getMessages().get(i).getData().get("line"), actual.getMessages().get(i).getData().get("line"));
            assertEquals(expected.getMessages().get(i).getData().get("from"), actual.getMessages().get(i).getData().get("from"));
        }
    }

    private Batch decodeCompressedBatch() {
        EmbeddedChannel channel = new EmbeddedChannel(new CompressedBatchEncoder(), new BeatsParser());
        channel.writeOutbound(this.batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        return (Batch) channel.readInbound();
    }

    private Batch decodeBatch() {
        EmbeddedChannel channel = new EmbeddedChannel(new BatchEncoder(), new BeatsParser());
        channel.writeOutbound(this.batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        return (Batch) channel.readInbound();
    }
}
