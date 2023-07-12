package org.logstash.beats;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;


public class BeatsParser extends ByteToMessageDecoder {
    private final static Logger logger = LogManager.getLogger(BeatsParser.class);

    private Batch batch;

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
    private boolean decodingCompressedBuffer = false;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws InvalidFrameProtocolException, IOException {
        if (!hasEnoughBytes(in)) {
            if (decodingCompressedBuffer) {
                throw new InvalidFrameProtocolException("Insufficient bytes in compressed content to decode: " + currentState);
            }
            return;
        }

        switch (currentState) {
            case READ_HEADER: {
                logger.trace("Running: READ_HEADER");

                int version = Protocol.version(in.readByte());

                if (batch == null) {
                    if (version == 2) {
                        batch = new V2Batch();
                        logger.trace("Frame version 2 detected");
                    } else {
                        logger.trace("Frame version 1 detected");
                        batch = new V1Batch();
                    }
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
                    default: {
                        throw new InvalidFrameProtocolException("Invalid Frame Type, received: " + frameType);
                    }
                }
                break;
            }
            case READ_WINDOW_SIZE: {
                logger.trace("Running: READ_WINDOW_SIZE");
                batch.setBatchSize((int) in.readUnsignedInt());

                // This is unlikely to happen but I have no way to known when a frame is
                // actually completely done other than checking the windows and the sequence number,
                // If the FSM read a new window and I have still
                // events buffered I should send the current batch down to the next handler.
                if(!batch.isEmpty()) {
                    logger.warn("New window size received but the current batch was not complete, sending the current batch");
                    out.add(batch);
                    batchComplete();
                } else if (batch.getBatchSize() == 0) {
                    logger.debug("New window size 0 received, sending an empty batch");
                    out.add(batch);
                    batchComplete();
                }

                transition(States.READ_HEADER);
                break;
            }
            case READ_DATA_FIELDS: {
                // Lumberjack version 1 protocol, which use the Key:Value format.
                logger.trace("Running: READ_DATA_FIELDS");
                sequence = (int) in.readUnsignedInt();
                int fieldsCount = (int) in.readUnsignedInt();
                int count = 0;

                if (fieldsCount <= 0) {
                    throw new InvalidFrameProtocolException("Invalid number of fields, received: " + fieldsCount);
                }

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
                ((V1Batch)batch).addMessage(message);

                if (batch.isComplete()){
                    out.add(batch);
                    batchComplete();
                }
                transition(States.READ_HEADER);

                break;
            }
            case READ_JSON_HEADER: {
                logger.trace("Running: READ_JSON_HEADER");

                sequence = (int) in.readUnsignedInt();
                int jsonPayloadSize = (int) in.readUnsignedInt();

                if(jsonPayloadSize <= 0) {
                    throw new InvalidFrameProtocolException("Invalid json length, received: " + jsonPayloadSize);
                }

                transition(States.READ_JSON, jsonPayloadSize);
                break;
            }
            case READ_COMPRESSED_FRAME_HEADER: {
                logger.trace("Running: READ_COMPRESSED_FRAME_HEADER");

                transition(States.READ_COMPRESSED_FRAME, in.readInt());
                break;
            }

            case READ_COMPRESSED_FRAME: {
                logger.trace("Running: READ_COMPRESSED_FRAME");

                inflateCompressedFrame(ctx, in, (buffer) -> {
                    transition(States.READ_HEADER);

                    decodingCompressedBuffer = true;
                    try {
                        while (buffer.readableBytes() > 0) {
                            decode(ctx, buffer, out);
                        }
                    } finally {
                        decodingCompressedBuffer = false;
                        transition(States.READ_HEADER);
                    }
                });
                break;
            }
            case READ_JSON: {
                logger.trace("Running: READ_JSON");
                ((V2Batch) batch).addMessage(sequence, in, requiredBytes);
                if (batch.isComplete()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Sending batch size: " + this.batch.size() + ", windowSize: " + batch.getBatchSize() + " , seq: " + sequence);
                    }
                    out.add(batch);
                    batchComplete();
                }

                transition(States.READ_HEADER);
                break;
            }
        }
    }

    private void inflateCompressedFrame(final ChannelHandlerContext ctx, final ByteBuf in, final CheckedConsumer<ByteBuf> fn)
        throws IOException {
        // Use the compressed size as the safe start for the buffer.
        ByteBuf buffer = ctx.alloc().buffer(requiredBytes);
        try {
            decompressImpl(in, buffer);
            fn.accept(buffer);
        } finally {
            buffer.release();
        }
    }

    private void decompressImpl(final ByteBuf in, final ByteBuf out) throws IOException {
        Inflater inflater = new Inflater();
        try (
                ByteBufOutputStream buffOutput = new ByteBufOutputStream(out);
                InflaterOutputStream inflaterStream = new InflaterOutputStream(buffOutput, inflater)
        ) {
            in.readBytes(inflaterStream, requiredBytes);
        } finally {
            inflater.end();
        }
    }

    private boolean hasEnoughBytes(ByteBuf in) {
        return in.readableBytes() >= requiredBytes;
    }

    private void transition(States next) {
        transition(next, next.length);
    }

    private void transition(States nextState, int requiredBytes) {
        if (logger.isTraceEnabled()) {
            logger.trace("Transition, from: " + currentState + ", to: " + nextState + ", requiring " + requiredBytes + " bytes");
        }
        this.currentState = nextState;
        this.requiredBytes = requiredBytes;
    }

    private void batchComplete() {
        requiredBytes = 0;
        sequence = 0;
        batch = null;
    }

    @FunctionalInterface
    private interface CheckedConsumer<T> {
        void accept(T t) throws IOException;
    }

}
