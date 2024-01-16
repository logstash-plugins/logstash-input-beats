package org.logstash.beats;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This handler is responsible to avoid accepting new connections when the direct memory
 * consumption is close to the MaxDirectMemorySize.
 * <p>
 * If the total allocated direct memory is close to the max memory size and also the pinned
 * bytes from the direct memory allocator is close to the direct memory used, then it drops the new
 * incoming connections.
 * */
@Sharable
public final class ThunderingGuardHandler extends ChannelInboundHandlerAdapter {

    private final static long MAX_DIRECT_MEMORY = io.netty.util.internal.PlatformDependent.maxDirectMemory();

    private final static Logger logger = LogManager.getLogger(ThunderingGuardHandler.class);

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        PooledByteBufAllocator pooledAllocator = (PooledByteBufAllocator) ctx.alloc();
        long usedDirectMemory = pooledAllocator.metric().usedDirectMemory();
        if (usedDirectMemory > MAX_DIRECT_MEMORY * 0.90) {
            long pinnedDirectMemory = pooledAllocator.pinnedDirectMemory();
            if (pinnedDirectMemory >= usedDirectMemory * 0.80) {
                ctx.close();
                logger.warn("Dropping connection {} due to high resource consumption", ctx.channel());
                return;
            }
        }

        super.channelRegistered(ctx);
    }
}
