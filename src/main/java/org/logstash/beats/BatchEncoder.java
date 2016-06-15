package org.logstash.beats;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;


public class BatchEncoder extends MessageToByteEncoder<Batch> {
    private final static Logger logger = LogManager.getLogger(BatchEncoder.class.getName());


    @Override
    protected void encode(ChannelHandlerContext ctx, Batch batch, ByteBuf out) throws Exception {
        out.writeByte(batch.getProtocol());
        out.writeByte('W');
        out.writeInt(batch.size());
        out.writeBytes(getPayload(ctx, batch));
    }

    protected ByteBuf getPayload(ChannelHandlerContext ctx, Batch batch) throws IOException {
        ByteBuf payload = ctx.alloc().buffer();

        // Aggregates the payload that we could decide to compress or not.
        for(Message message : batch.getMessages()) {
            if (batch.getProtocol() == Protocol.VERSION_2) {
                encodeMessageWithJson(payload, message);
            } else {
                encodeMessageWithFields(payload, message);
            }
        }
        return payload;
    }

    protected void encodeMessageWithJson(ByteBuf payload, Message message) throws JsonProcessingException {
        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte('J');
        payload.writeInt(message.getSequence());

        byte[] json = JsonUtils.mapper.writeValueAsBytes(message.getData());
        payload.writeInt(json.length);
        payload.writeBytes(json);
    }

    protected void encodeMessageWithFields(ByteBuf payload, Message message) {
        payload.writeByte(Protocol.VERSION_1);
        payload.writeByte('D');
        payload.writeInt(message.getSequence());
        payload.writeInt(message.getData().size());

        for(Object entry : message.getData().entrySet()) {
            Map.Entry e = (Map.Entry) entry;
            byte[] key = ((String) e.getKey()).getBytes();
            byte[] value = ((String) e.getValue()).getBytes();

            logger.debug("New entry: key l: " + key.length  + ", value: " + value.length);

            payload.writeInt(key.length);
            payload.writeBytes(key);
            payload.writeInt(value.length);
            payload.writeBytes(value);
        }
    }
}
