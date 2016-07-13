package org.logstash.beats;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.InflaterOutputStream;


public class BeatsParser extends ByteToMessageDecoder {
    private static final int CHUNK_SIZE = 1024;
    public final static ObjectMapper MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());
    private final static Logger logger = LogManager.getLogger(Server.class.getName());

    private Batch batch = new Batch();

    private enum States {
        READ_HEADER(1),
        READ_FRAME_TYPE(1),
        READ_WINDOW_SIZE(4),
        READ_JSON_HEADER(8),
        READ_COMPRESSED_FRAME_HEADER(4),
        READ_COMPRESSED_FRAME(-1), // -1 means the length to read is variable and defined in the frame itself.
        READ_JSON(-1),
        READ_DATA_FIELDS(-1);

        private int length;

        States(int length) {
            this.length = length;
        }

    }

    private States currentState = States.READ_HEADER;
    private int requiredBytes = 0;
    private int sequence = 0;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if(!hasEnoughBytes(in)) {
            return;
        }

        switch (currentState) {
            case READ_HEADER: {
                logger.debug("Running: READ_HEADER");

                byte currentVersion = in.readByte();

                if(Protocol.isVersion2(currentVersion)) {
                    logger.debug("Frame version 2 detected");
                    batch.setProtocol(Protocol.VERSION_2);
                } else {
                    logger.debug("Frame version 1 detected");
                    batch.setProtocol(Protocol.VERSION_1);
                }

                transition(States.READ_FRAME_TYPE);
                break;
            }
            case READ_FRAME_TYPE: {
                byte frameType = in.readByte();

                switch(frameType) {
                    case Protocol.CODE_WINDOW_SIZE: {
                        transition(States.READ_WINDOW_SIZE);
                        break;
                    }
                    case Protocol.CODE_JSON_FRAME: {
                        // Reading Sequence + size of the payload
                        transition(States.READ_JSON_HEADER);
                        break;
                    }
                    case Protocol.CODE_COMPRESSED_FRAME: {
                        transition(States.READ_COMPRESSED_FRAME_HEADER);
                        break;
                    }
                    case Protocol.CODE_FRAME: {
                        transition(States.READ_DATA_FIELDS);
                        break;
                    }
                }
                break;
            }
            case READ_WINDOW_SIZE: {
                logger.debug("Running: READ_WINDOW_SIZE");

                batch.setBatchSize((int) in.readUnsignedInt());

                // This is unlikely to happen but I have no way to known when a frame is
                // actually completely done other than checking the windows and the sequence number,
                // If the FSM read a new window and I have still
                // events buffered I should send the current batch down to the next handler.
                if(!batch.isEmpty()) {
                    logger.warn("New window size received but the current batch was not complete, sending the current batch");
                    out.add(batch);
                    batchComplete();
                }

                transition(States.READ_HEADER);
                break;
            }
            case READ_DATA_FIELDS: {
                // Lumberjack version 1 protocol, which use the Key:Value format.
                logger.debug("Running: READ_DATA_FIELDS");
                sequence = (int) in.readUnsignedInt();
                int fieldsCount = (int) in.readUnsignedInt();
                int count = 0;

                Map dataMap = new HashMap<String, String>(fieldsCount);

                while(count < fieldsCount) {
                    int fieldLength = (int) in.readUnsignedInt();
                    ByteBuf fieldBuf = in.readBytes(fieldLength);
                    String field = fieldBuf.toString(Charset.forName("UTF8"));
                    fieldBuf.release();

                    int dataLength = (int) in.readUnsignedInt();
                    ByteBuf dataBuf = in.readBytes(dataLength);
                    String data = dataBuf.toString(Charset.forName("UTF8"));
                    dataBuf.release();

                    dataMap.put(field, data);

                    count++;
                }

                Message message = new Message(sequence, dataMap);
                batch.addMessage(message);

                if(batch.complete()) {
                    out.add(batch);
                    batchComplete();
                }

                transition(States.READ_HEADER);

                break;
            }
            case READ_JSON_HEADER: {
                logger.debug("Running: READ_JSON_HEADER");

                sequence = (int) in.readUnsignedInt();
                int jsonPayloadSize = (int) in.readUnsignedInt();

                transition(States.READ_JSON, jsonPayloadSize);
                break;
            }
            case READ_COMPRESSED_FRAME_HEADER: {
                logger.debug("Running: READ_COMPRESSED_FRAME_HEADER");

                transition(States.READ_COMPRESSED_FRAME, in.readInt());
                break;
            }

            case READ_COMPRESSED_FRAME: {
                logger.debug("Running: READ_COMPRESSED_FRAME");
                // Use the compressed size as the safe start for the buffer.
                ByteBuf buffer = ctx.alloc().buffer(requiredBytes);
                try (
                        ByteBufOutputStream buffOutput = new ByteBufOutputStream(buffer);
                        InflaterOutputStream inflater = new InflaterOutputStream(buffOutput, new Inflater());
                ) {
                    ByteBuf bytesRead = in.readBytes(inflater, requiredBytes);
                    transition(States.READ_HEADER);
                    try {
                        while (buffer.readableBytes() > 0) {
                            decode(ctx, buffer, out);
                        }
                    } finally {
                        buffer.release();
                    }
                }

                break;
            }
            case READ_JSON: {
                logger.debug("Running: READ_JSON");

                byte[] bytes = new byte[requiredBytes];
                in.readBytes(bytes);
                Message message = new Message(sequence, (Map) MAPPER.readValue(bytes, Object.class));

                batch.addMessage(message);

                if(batch.size() == batch.getBatchSize()) {
                    logger.debug("Sending batch size: {}, windowSize: {} , seq: {}", batch.size(), batch.getBatchSize(), sequence);
                    out.add(batch);
                    batchComplete();
                }

                transition(States.READ_HEADER);
                break;
            }
        }
    }

    private boolean hasEnoughBytes(ByteBuf in) {
        return in.readableBytes() >= requiredBytes;
    }

    private void transition(States next) {
        transition(next, next.length);
    }

    private void transition(States nextState, int requiredBytes) {
        logger.debug("Transition, from: {}, to: {}, requiring {} bytes", currentState, nextState, requiredBytes);
        this.currentState = nextState;
        this.requiredBytes = requiredBytes;
    }

    private void batchComplete() {
        requiredBytes = 0;
        sequence = 0;
        batch = new Batch();
    }
}
