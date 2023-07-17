package org.logstash.beats;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.PlatformDependent;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ThunderingGuardHandlerTest {

    public static final int MB = 1024 * 1024;
    public static final long MAX_DIRECT_MEMORY_BYTES = PlatformDependent.maxDirectMemory();

    @Test
    public void testVerifyDirectMemoryCouldGoBeyondThe90Percent() {
        // allocate 90% of direct memory
        List<ByteBuf> allocatedBuffers = allocateDirectMemory(MAX_DIRECT_MEMORY_BYTES, 0.9);

        // allocate one more
        ByteBuf payload = PooledByteBufAllocator.DEFAULT.directBuffer(1 * MB);
        long usedDirectMemory = PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory();
        long pinnedDirectMemory = PooledByteBufAllocator.DEFAULT.pinnedDirectMemory();

        // verify
        assertTrue("Direct memory allocation should be > 90% of the max available", usedDirectMemory > 0.9 * MAX_DIRECT_MEMORY_BYTES);
        assertTrue("Direct memory usage should be > 80% of the max available", pinnedDirectMemory > 0.8 * MAX_DIRECT_MEMORY_BYTES);

        allocatedBuffers.forEach(ReferenceCounted::release);
        payload.release();
    }

    private static List<ByteBuf> allocateDirectMemory(long maxDirectMemoryBytes, double percentage) {
        List<ByteBuf> allocatedBuffers = new ArrayList<>();
        final long numBuffersToAllocate = (long) (maxDirectMemoryBytes / MB * percentage);
        for (int i = 0; i < numBuffersToAllocate; i++) {
            allocatedBuffers.add(PooledByteBufAllocator.DEFAULT.directBuffer(1 * MB));
        }
        return allocatedBuffers;
    }

    @Test
    public void givenUsedDirectMemoryAndPinnedMemoryAreCloseToTheMaxDirectAvailableWhenNewConnectionIsCreatedThenItIsReject() {
        EmbeddedChannel channel = new EmbeddedChannel(new ThunderingGuardHandler());

        // consume > 90% of the direct memory
        List<ByteBuf> allocatedBuffers = allocateDirectMemory(MAX_DIRECT_MEMORY_BYTES, 0.9);
        // allocate one more
        ByteBuf payload = PooledByteBufAllocator.DEFAULT.directBuffer(1 * MB);

        channel.pipeline().fireChannelRegistered();

        // verify
        assertFalse("Under constrained memory new channels has to be forcibly closed", channel.isOpen());

        allocatedBuffers.forEach(ReferenceCounted::release);
        payload.release();
    }

    @Test
    public void givenUsedDirectMemoryAndNotPinnedWhenNewConnectionIsCreatedThenItIsAccepted() {
        EmbeddedChannel channel = new EmbeddedChannel(new ThunderingGuardHandler());

        // consume > 90% of the direct memory
        List<ByteBuf> allocatedBuffers = allocateDirectMemory(MAX_DIRECT_MEMORY_BYTES, 0.9);
        allocatedBuffers.forEach(ReferenceCounted::release);
        // allocate one more
        ByteBuf payload = PooledByteBufAllocator.DEFAULT.directBuffer(1 * MB);
        payload.release();

        channel.pipeline().fireChannelRegistered();

        // verify
        assertTrue("Despite memory is allocated but not pinned, new connections MUST be accepted", channel.isOpen());

    }

}