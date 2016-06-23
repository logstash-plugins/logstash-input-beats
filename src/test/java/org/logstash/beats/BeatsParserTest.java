package org.logstash.beats;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class BeatsParserTest {
    private Batch batch;
    private Message message1;
    private Message message2;
    private final int numberOfMessage = 20;

    @Before
    public void setup() {
        this.batch = new Batch();
        this.batch.setProtocol(Protocol.VERSION_2);

        Map map1 = new HashMap<String, String>();
        map1.put("line", "My super event log");
        map1.put("from", "beats");

        this.message1 = new Message(1, map1);
        this.batch.addMessage(this.message1);

        for(int i = 0; i < numberOfMessage; i++) {
            Map map2 = new HashMap<String, String>();
            map2.put("line", "Another world");
            map2.put("from", "Little big Adventure");

            this.message2 = new Message(i + 1, map2);
            this.batch.addMessage(this.message2);
        }
    }

    @Test
    public void testEncodingDecodingJson() {
        EmbeddedChannel channel = new EmbeddedChannel(new BatchEncoder(), new BeatsParser());
        channel.writeOutbound(this.batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        Batch decodedBatch = channel.readInbound();
        assertMessages(this.batch, decodedBatch);
    }

    @Test
    public void testCompressedEncodingDecodingJson() {
        EmbeddedChannel channel = new EmbeddedChannel(new CompressedBatchEncoder(), new BeatsParser());
        channel.writeOutbound(this.batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        Batch decodedBatch = (Batch) channel.readInbound();
        assertMessages(this.batch, decodedBatch);
    }

    @Test
    public void testEncodingDecodingFields() {
        this.batch.setProtocol(Protocol.VERSION_1);

        EmbeddedChannel channel = new EmbeddedChannel(new BatchEncoder(), new BeatsParser());
        channel.writeOutbound(this.batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        Batch decodedBatch = channel.readInbound();
        assertMessages(this.batch, decodedBatch);
    }


    @Test
    public void  testCompressedEncodingDecodingFields() {
        this.batch.setProtocol(Protocol.VERSION_1);

        EmbeddedChannel channel = new EmbeddedChannel(new CompressedBatchEncoder(), new BeatsParser());
        channel.writeOutbound(this.batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        Batch decodedBatch = (Batch) channel.readInbound();

        assertMessages(this.batch, decodedBatch);
    }

    private void assertMessages(Batch expected, Batch actual) {
        assertNotNull(actual);
        assertEquals(expected.getMessages().get(0).getSequence(), actual.getMessages().get(0).getSequence());
        assertEquals(expected.getMessages().get(1).getSequence(), actual.getMessages().get(1).getSequence());
        assertEquals(expected.getMessages().get(0).getData().get("line"), actual.getMessages().get(0).getData().get("line"));
        assertEquals(expected.getMessages().get(1).getData().get("line"), actual.getMessages().get(1).getData().get("line"));
        assertEquals(expected.getMessages().get(0).getData().get("from"), actual.getMessages().get(0).getData().get("from"));
        assertEquals(expected.getMessages().get(1).getData().get("from"), actual.getMessages().get(1).getData().get("from"));
        assertEquals(expected.size(), actual.size());
    }
}
