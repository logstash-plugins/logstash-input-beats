package org.logstash.beats;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KeepAliveHandler extends ChannelDuplexHandler {
    private final static Logger logger = LogManager.getLogger(KeepAliveHandler.class);

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        IdleStateEvent e = null;

        if (evt instanceof IdleStateEvent) {
            e = (IdleStateEvent) evt;
            if (e.state() == IdleState.WRITER_IDLE) {
                if (isProcessing(ctx)) {
                    logger.trace("keep alive");
                    ChannelFuture f = ctx.writeAndFlush(new Ack(Protocol.VERSION_2, 0));
                    if (logger.isTraceEnabled()) {
                        f.addListener((ChannelFutureListener) future -> {
                            if (future.isSuccess()) {
                                logger.trace("Keep alive Success");
                            } else {
                                logger.trace("Not success");
                            }
                        });
                    }
                }
            } else if (e.state() == IdleState.ALL_IDLE) {
                logger.debug("idle state, all idle, timeout reaching could if this happen it could mean logstash is under backpressure.");
                ctx.close();
            }
        }
    }

    public boolean isProcessing(ChannelHandlerContext ctx) {
        return ctx.channel().attr(BeatsHandler.PROCESSING_BATCH).get();
    }
}