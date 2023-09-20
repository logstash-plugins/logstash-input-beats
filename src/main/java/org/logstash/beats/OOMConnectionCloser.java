package org.logstash.beats;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.PlatformDependent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OOMConnectionCloser extends ChannelInboundHandlerAdapter {

    private final PooledByteBufAllocator allocator;

    static class DirectMemoryUsage {
        final long used;
        final long pinned;
        private final PooledByteBufAllocator allocator;
        final short ratio;

        private DirectMemoryUsage(long used, long pinned, PooledByteBufAllocator allocator) {
            this.used = used;
            this.pinned = pinned;
            this.allocator = allocator;
            this.ratio = (short) Math.round(((double) pinned / used) * 100);
        }

        static DirectMemoryUsage capture(PooledByteBufAllocator allocator) {
            long usedDirectMemory = allocator.metric().usedDirectMemory();
            long pinnedDirectMemory = allocator.pinnedDirectMemory();
            return new DirectMemoryUsage(usedDirectMemory, pinnedDirectMemory, allocator);
        }

        boolean isCloseToOOM() {
            long maxDirectMemory = PlatformDependent.maxDirectMemory();
            int chunkSize = allocator.metric().chunkSize();
            return ((maxDirectMemory - used) <= chunkSize) && ratio > 75;
        }
    }

    private final static Logger logger = LogManager.getLogger(OOMConnectionCloser.class);

    public static final Pattern DIRECT_MEMORY_ERROR = Pattern.compile("^Cannot reserve \\d* bytes of direct buffer memory.*$");

    OOMConnectionCloser() {
        allocator = (PooledByteBufAllocator) ByteBufAllocator.DEFAULT;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        DirectMemoryUsage direct = DirectMemoryUsage.capture(allocator);
        logger.info("Direct memory status, used: {}, pinned: {}, ratio: {}", direct.used, direct.pinned, direct.ratio);
        if (direct.isCloseToOOM()) {
            logger.warn("Closing connection {} because running out of memory, used: {}, pinned: {}, ratio {}", ctx.channel(), direct.used, direct.pinned, direct.ratio);
            ReferenceCountUtil.release(msg); // to free the memory used by the buffer
            ctx.flush();
            ctx.close();
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (isDirectMemoryOOM(cause)) {
            DirectMemoryUsage direct = DirectMemoryUsage.capture(allocator);
            logger.info("Direct memory status, used: {}, pinned: {}, ratio: {}", direct.used, direct.pinned, direct.ratio);
            logger.warn("Dropping connection {} due to lack of available Direct Memory. Please lower the number of concurrent connections or reduce the batch size. " +
                    "Alternatively, raise -XX:MaxDirectMemorySize option in the JVM running Logstash", ctx.channel());
            ctx.flush();
            ctx.close();
        } else {
            super.exceptionCaught(ctx, cause);
        }
    }

    private boolean isDirectMemoryOOM(Throwable th) {
        if (!(th instanceof OutOfMemoryError)) {
            return false;
        }
        Matcher m = DIRECT_MEMORY_ERROR.matcher(th.getMessage());
        return m.matches();
    }
}