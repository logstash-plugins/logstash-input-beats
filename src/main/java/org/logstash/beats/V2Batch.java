package org.logstash.beats;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Implementation of {@link Batch} for the v2 protocol backed by ByteBuf. *must* be released after use.
 */
public class V2Batch implements Batch {

    private final static Logger logger = LogManager.getLogger(V2Batch.class);
    private static volatile int LAST_NOTIFIED_MAX_ORDER = -1;
    // The value 14 comes from PooledByteBufAllocator.validateAndCalculateChunkSize
    private static final int NETTY_MAXIMUM_ORDER = 14;

    private ByteBuf internalBuffer = PooledByteBufAllocator.DEFAULT.buffer();
    private int written = 0;
    private int read = 0;
    private static final int SIZE_OF_INT = 4;
    private int batchSize;
    private int highestSequence = -1;

    public void setProtocol(byte protocol) {
        if (protocol != Protocol.VERSION_2) {
            throw new IllegalArgumentException("Only version 2 protocol is supported");
        }
    }

    @Override
    public byte getProtocol() {
        return Protocol.VERSION_2;
    }

    public Iterator<Message> iterator() {
        internalBuffer.resetReaderIndex();
        return new Iterator<Message>() {
            @Override
            public boolean hasNext() {
                return read < written;
            }

            @Override
            public Message next() {
                int sequenceNumber = internalBuffer.readInt();
                int readableBytes = internalBuffer.readInt();
                Message message = new Message(sequenceNumber, internalBuffer.slice(internalBuffer.readerIndex(), readableBytes));
                internalBuffer.readerIndex(internalBuffer.readerIndex() + readableBytes);
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
    public int getHighestSequence() {
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
        if (internalBuffer.writableBytes() < size + (2 * SIZE_OF_INT)) {
            int requiredSize = internalBuffer.capacity() + size + (2 * SIZE_OF_INT);
            eventuallyLogIdealMaxOrder(requiredSize, logger);

            internalBuffer.capacity(requiredSize);
        }
        internalBuffer.writeInt(sequenceNumber);
        internalBuffer.writeInt(size);
        buffer.readBytes(internalBuffer, size);
        if (sequenceNumber > highestSequence) {
            highestSequence = sequenceNumber;
        }
    }

    // package-private for testability reasons
    void eventuallyLogIdealMaxOrder(int requiredSize, Logger logger) {
        int idealMaxOrder = idealMaxOrder(requiredSize);
        if (idealMaxOrder <= PooledByteBufAllocator.defaultMaxOrder()) {
            return;
        }
        if (!needsToBeLogged(idealMaxOrder)) {
            return;
        }

        if (idealMaxOrder > NETTY_MAXIMUM_ORDER) {
            logger.error("Received batch of size {} bytes that is too large to fit into the pre-allocated memory pool. Reduce the size of the batch to improve performance and avoid data loss.", requiredSize);
        } else {
            logger.warn("Received batch of size {} bytes that is too large to fit into the pre-allocated memory pool. This will cause a performance degradation. Set 'io.netty.allocator.maxOrder' JVM property to {} to accommodate batches bigger than {} bytes.",
                    requiredSize, idealMaxOrder, PooledByteBufAllocator.DEFAULT.metric().chunkSize());
        }
        trackAsAlreadyLogged(idealMaxOrder);
    }

    private void trackAsAlreadyLogged(int maxOrder) {
        LAST_NOTIFIED_MAX_ORDER = capMaxOrder(maxOrder);
    }

    private boolean needsToBeLogged(int maxOrder) {
        return capMaxOrder(maxOrder) > LAST_NOTIFIED_MAX_ORDER;
    }

    private static int capMaxOrder(int maxOrder) {
        return maxOrder > NETTY_MAXIMUM_ORDER ? Integer.MAX_VALUE : maxOrder;
    }

    /**
     * Return the ideal maxOrder value to configure chunks of size where a buffer of requiredSize can fit.
     * */
    private int idealMaxOrder(int requiredSize) {
        int chunkSize = PooledByteBufAllocator.DEFAULT.metric().chunkSize();
        int defaultMaxOrder = PooledByteBufAllocator.defaultMaxOrder();
        int defaultPageSize = PooledByteBufAllocator.defaultPageSize();
        if (requiredSize > chunkSize) {
            int nextMaxOrder = defaultMaxOrder;
            do {
                nextMaxOrder ++;
            } while (requiredSize > (defaultPageSize << nextMaxOrder));
            return nextMaxOrder;
        } else {
            return defaultMaxOrder;
        }
    }

    @Override
    public void release() {
        internalBuffer.release();
    }

    // visible for testing
    static void resetReportedOrders() {
        LAST_NOTIFIED_MAX_ORDER = -1;
    }
}
