package org.logstash.beats;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
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
    public void testEncodingDecodingFieldWithUTFCharacters() {
        this.batch = new Batch();
        this.batch.setProtocol(Protocol.VERSION_2);

        // Generate Data with Keys and String with UTF-8
        for(int i = 0; i < numberOfMessage; i++) {
            Map map = new HashMap<String, String>();
            map.put("étoile", "mystère");
            map.put("from", "ÉeèAççï");

            Message message = new Message(i + 1, map);
            this.batch.addMessage(message);
        }

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
        thrown.expectCause(isA(BeatsParser.InvalidFrameProtocolException.class));

        byte[] n = new byte[10000];
        new Random().nextBytes(n);
        ByteBuf randomBufferData = Unpooled.wrappedBuffer(n);

        sendPayloadToParser(randomBufferData);
    }


    @Test
    public void testNegativeJsonPayloadShouldRaiseAnException() throws JsonProcessingException {
        sendInvalidJSonPayload(-1);
    }

    @Test
    public void testZeroSizeJsonPayloadShouldRaiseAnException() throws JsonProcessingException {
        sendInvalidJSonPayload(0);
    }

    @Test
    public void testNegativeFieldsCountShouldRaiseAnException() {
        sendInvalidV1Payload(-1);
    }

    @Test
    public void testZeroFieldsCountShouldRaiseAnException() {
        sendInvalidV1Payload(0);
    }

    private void sendInvalidV1Payload(int size) {
        thrown.expectCause(isA(BeatsParser.InvalidFrameProtocolException.class));
        thrown.expectMessage("Invalid number of fields, received: " + size);

        ByteBuf payload = Unpooled.buffer();

        payload.writeByte(Protocol.VERSION_1);
        payload.writeByte('W');
        payload.writeInt(1);
        payload.writeByte(Protocol.VERSION_1);
        payload.writeByte('D');
        payload.writeInt(1);
        payload.writeInt(size);

        byte[] key = "message".getBytes();
        byte[] value = "Hola".getBytes();

        payload.writeInt(key.length);
        payload.writeBytes(key);
        payload.writeInt(value.length);
        payload.writeBytes(value);

        sendPayloadToParser(payload);
    }

    private void sendInvalidJSonPayload(int size) throws JsonProcessingException {
        thrown.expectCause(isA(BeatsParser.InvalidFrameProtocolException.class));
        thrown.expectMessage("Invalid json length, received: " + size);

        Map mapData = Collections.singletonMap("message", "hola");

        ByteBuf payload = Unpooled.buffer();

        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte('W');
        payload.writeInt(1);
        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte('J');
        payload.writeInt(1);
        payload.writeInt(size);

        byte[] json = BeatsParser.MAPPER.writeValueAsBytes(mapData);
        payload.writeBytes(json);

        sendPayloadToParser(payload);
    }

    private void sendPayloadToParser(ByteBuf payload) {
        EmbeddedChannel channel = new EmbeddedChannel(new BeatsParser());
        channel.writeOutbound(payload);
        Object o = channel.readOutbound();
        channel.writeInbound(o);
    }

    private void assertMessages(Batch expected, Batch actual) {
        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());

        for(int i=0; i < expected.size(); i++) {
            assertEquals(expected.getMessages().get(i).getSequence(), actual.getMessages().get(i).getSequence());

            Map expectedData = expected.getMessages().get(i).getData();
            Map actualData = actual.getMessages().get(i).getData();

            assertEquals(expectedData.size(), actualData.size());

            for(Object entry : expectedData.entrySet()) {
                Map.Entry e = (Map.Entry) entry;
                String key = (String) e.getKey();
                String value = (String) e.getValue();

                assertEquals(value, actualData.get(key));
            }
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