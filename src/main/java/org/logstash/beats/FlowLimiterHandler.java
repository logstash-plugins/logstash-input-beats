package org.logstash.beats;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Configure the channel where it's installed to operate the read in pull mode,
 * disabling the autoread and explicitly invoking the read operation.
 * */
@ChannelHandler.Sharable
public final class FlowLimiterHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        ctx.channel().config().setAutoRead(false);
        super.channelRegistered(ctx);
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        if (!ctx.channel().config().isAutoRead()) {
            ctx.channel().read();
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        if (!ctx.channel().config().isAutoRead()) {
            ctx.channel().read();
        }
    }
}
