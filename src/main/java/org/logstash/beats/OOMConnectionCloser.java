package org.logstash.beats;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OOMConnectionCloser extends ChannelInboundHandlerAdapter {

    private static class DirectMemoryUsage {
        private final long used;
        private final long pinned;
        private final short ratio;

        private DirectMemoryUsage(long used, long pinned) {
            this.used = used;
            this.pinned = pinned;
            this.ratio = (short) Math.round(((double) pinned / used) * 100);
        }

        static DirectMemoryUsage capture() {
            PooledByteBufAllocator allocator = (PooledByteBufAllocator) ByteBufAllocator.DEFAULT;
            long usedDirectMemory = allocator.metric().usedDirectMemory();
            long pinnedDirectMemory = allocator.pinnedDirectMemory();
            return new DirectMemoryUsage(usedDirectMemory, pinnedDirectMemory);
        }
    }

    private final static Logger logger = LogManager.getLogger(OOMConnectionCloser.class);

    public static final Pattern DIRECT_MEMORY_ERROR = Pattern.compile("^Cannot reserve \\d* bytes of direct buffer memory.*$");

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (isDirectMemoryOOM(cause)) {
            DirectMemoryUsage direct = DirectMemoryUsage.capture();
            logger.info("Direct memory status, used: {}, pinned: {}, ratio: {}", direct.used, direct.pinned, direct.ratio);
            logger.warn("Dropping connection {} because run out of direct memory. To fix it, in Filebeat you can" +
                    "enable slow_start, reduce number of workers, reduce the bulk_max_size or even raise up the -XX:MaxDirectMemorySize " +
                    "option in the JVM running Logstash", ctx.channel());
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