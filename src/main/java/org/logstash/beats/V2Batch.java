package org.logstash.beats;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Implementation of {@link Batch} for the v2 protocol backed by ByteBuf. *must* be released after use.
 */
public class V2Batch implements Batch, Closeable {
    private ByteBuf internalBuffer = PooledByteBufAllocator.DEFAULT.buffer();
    private int written = 0;
    private static final int SIZE_OF_INT = 4;
    private int batchSize;
    private int highestSequence = -1;

    public void setProtocol(byte protocol){
        if (protocol != Protocol.VERSION_2){
            throw new IllegalArgumentException("Only version 2 protocol is supported");
        }
    }

    @Override
    public byte getProtocol() {
        return Protocol.VERSION_2;
    }

    public Iterator<Message> iterator() {
        return new Iterator<Message>() {
            private int read = 0;
            private ByteBuf readerBuffer = internalBuffer.asReadOnly();
            {
                readerBuffer.resetReaderIndex();
            }
            @Override
            public boolean hasNext() {
                return read < written;
            }

            @Override
            public Message next() {
                if (read >= written) {
                    throw new NoSuchElementException();
                }
                int sequenceNumber = readerBuffer.readInt();
                int readableBytes = readerBuffer.readInt();
                Message message = new Message(sequenceNumber, readerBuffer.slice(readerBuffer.readerIndex(), readableBytes));
                readerBuffer.readerIndex(readerBuffer.readerIndex() + readableBytes);
                message.setBatch(V2Batch.this);
                read++;
                return message;
            }
        };
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public int size() {
        return written;
    }

    @Override
    public boolean isEmpty() {
        return written == 0;
    }

    @Override
    public boolean isComplete() {
        return written == batchSize;
    }

    @Override
    public int getHighestSequence(){
        return highestSequence;
    }

    /**
     * Adds a message to the batch, which will be constructed into an actual {@link Message} lazily.
      * @param sequenceNumber sequence number of the message within the batch
     * @param buffer A ByteBuf pointing to serialized JSon
     * @param size size of the serialized Json
     */
    void addMessage(int sequenceNumber, ByteBuf buffer, int size) {
        written++;
        if (internalBuffer.writableBytes() < size + (2 * SIZE_OF_INT)){
            internalBuffer.capacity(internalBuffer.capacity() + size + (2 * SIZE_OF_INT));
        }
        internalBuffer.writeInt(sequenceNumber);
        internalBuffer.writeInt(size);
        buffer.readBytes(internalBuffer, size);
        if (sequenceNumber > highestSequence){
            highestSequence = sequenceNumber;
        }
    }

    @Override
    public void release() {
        internalBuffer.release();
    }

    @Override
    public void close() {
        release();
    }

}
