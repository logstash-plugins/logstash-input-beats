package org.logstash.beats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class BeatsParserTest {
    private V1Batch v1Batch;
    private V2Batch byteBufBatch;
    public final static ObjectMapper MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());

    private final int numberOfMessage = 20;

    @Rule
    public ExpectedException thrown = ExpectedException.none();


    @Before
    public void setup() throws Exception{
        this.v1Batch = new V1Batch();

        for(int i = 1; i <= numberOfMessage; i++) {
            Map map = new HashMap<String, String>();
            map.put("line", "Another world");
            map.put("from", "Little big Adventure");

            Message message = new Message(i, map);
            this.v1Batch.addMessage(message);
        }

        this.byteBufBatch = new V2Batch();

        for(int i = 1; i <= numberOfMessage; i++) {
            Map map = new HashMap<String, String>();
            map.put("line", "Another world");
            map.put("from", "Little big Adventure");
            ByteBuf bytebuf = Unpooled.wrappedBuffer(MAPPER.writeValueAsBytes(map));
            this.byteBufBatch.addMessage(i, bytebuf, bytebuf.readableBytes());
        }

    }

    @Test
    public void testEncodingDecodingJson() {
        Batch decodedBatch = decodeBatch(v1Batch);
        assertMessages(v1Batch, decodedBatch);
    }

    @Test
    public void testCompressedEncodingDecodingJson() {
        Batch decodedBatch = decodeCompressedBatch(v1Batch);
        assertMessages(v1Batch, decodedBatch);
    }

    @Test
    public void testEncodingDecodingFields() {
        Batch decodedBatch = decodeBatch(v1Batch);
        assertMessages(v1Batch, decodedBatch);
    }

    @Test
    public void testEncodingDecodingFieldWithUTFCharacters() throws Exception {
        V2Batch v2Batch = new V2Batch();


        // Generate Data with Keys and String with UTF-8
        for(int i = 0; i < numberOfMessage; i++) {
            ByteBuf payload = Unpooled.buffer();

            Map map = new HashMap<String, String>();
            map.put("étoile", "mystère");
            map.put("from", "ÉeèAççï");

            byte[] json = MAPPER.writeValueAsBytes(map);
            payload.writeBytes(json);

            v2Batch.addMessage(i, payload, payload.readableBytes());
        }

        Batch decodedBatch = decodeBatch(v2Batch);
        assertMessages(v2Batch, decodedBatch);
    }

    @Test
    public void testV1EncodingDecodingFieldWithUTFCharacters() {
        V1Batch batch = new V1Batch();

        // Generate Data with Keys and String with UTF-8
        for(int i = 0; i < numberOfMessage; i++) {

            Map map = new HashMap<String, String>();
            map.put("étoile", "mystère");
            map.put("from", "ÉeèAççï");

            Message message = new Message(i + 1, map);
            batch.addMessage(message);
        }

        Batch decodedBatch = decodeBatch(batch);
        assertMessages(batch, decodedBatch);
    }

    @Test
    public void testCompressedEncodingDecodingFields() {
        Batch decodedBatch = decodeCompressedBatch(v1Batch);
        assertMessages(this.v1Batch, decodedBatch);
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

        byte[] json = MAPPER.writeValueAsBytes(mapData);
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

        int i = 0;
        Iterator<Message> expectedMessages = expected.iterator();
        for(Message actualMessage: actual) {
            Message expectedMessage = expectedMessages.next();
            assertEquals(expectedMessage.getSequence(), actualMessage.getSequence());

            Map expectedData = expectedMessage.getData();
            Map actualData = actualMessage.getData();

            assertEquals(expectedData.size(), actualData.size());

            for(Object entry : expectedData.entrySet()) {
                Map.Entry e = (Map.Entry) entry;
                String key = (String) e.getKey();
                String value = (String) e.getValue();

                assertEquals(value, actualData.get(key));
            }
        }
    }

    private Batch decodeCompressedBatch(Batch batch) {
        EmbeddedChannel channel = new EmbeddedChannel(new CompressedBatchEncoder(), new BeatsParser());
        channel.writeOutbound(batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        return (Batch) channel.readInbound();
    }

    private Batch decodeBatch(Batch batch) {
        EmbeddedChannel channel = new EmbeddedChannel(new BatchEncoder(), new BeatsParser());
        channel.writeOutbound(batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        return (Batch) channel.readInbound();
    }
}