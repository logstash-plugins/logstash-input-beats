package org.logstash.beats;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Configure the channel where it's installed to operate the read in pull mode,
 * disabling the autoread and explicitly invoking the read operation.
 * The flow control to keep the outgoing buffer under control is done
 * avoiding to read in new bytes if the outgoing direction became not writable, this
 * excert back pressure to the TCP layer and ultimately to the upstream system.
 * */
@ChannelHandler.Sharable
public final class FlowLimiterHandler extends ChannelInboundHandlerAdapter {

    private final static Logger logger = LogManager.getLogger(FlowLimiterHandler.class);

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        ctx.channel().config().setAutoRead(false);
        super.channelRegistered(ctx);
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        if (isAutoreadDisabled(ctx.channel()) && ctx.channel().isWritable()) {
            ctx.channel().read();
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        if (isAutoreadDisabled(ctx.channel()) && ctx.channel().isWritable()) {
            ctx.channel().read();
        }
    }

    private boolean isAutoreadDisabled(Channel channel) {
        return !channel.config().isAutoRead();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().read();
        super.channelWritabilityChanged(ctx);

        logger.debug("Writability on channel {} changed to {}", ctx.channel(), ctx.channel().isWritable());
    }

}
