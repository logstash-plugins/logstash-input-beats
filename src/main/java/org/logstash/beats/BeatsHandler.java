package org.logstash.beats;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import org.apache.log4j.Logger;

@ChannelHandler.Sharable
public class BeatsHandler extends SimpleChannelInboundHandler<Batch> {
    private final static Logger logger = Logger.getLogger(BeatsHandler.class);
    private final IMessageListener messageListener;
    private final static AttributeKey<Boolean> PROCESSING = AttributeKey.newInstance("processing");

    public BeatsHandler(IMessageListener listener) {
        messageListener = listener;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(PROCESSING).set(false);
        messageListener.onNewConnection(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        messageListener.onConnectionClose(ctx);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Batch batch) throws Exception {
        logger.debug("Received a new payload");

        ctx.channel().attr(PROCESSING).set(true);
        try {
            for(Message message : batch.getMessages()) {
                logger.debug("Sending a new message for the listener, sequence: " + message.getSequence());
                messageListener.onNewMessage(ctx, message);

                if(needAck(message)) {
                    ack(ctx, message);
                }
            }
            ctx.flush();
        } finally {
            ctx.channel().attr(PROCESSING).set(false);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        messageListener.onException(ctx, cause);
        logger.error("Exception: " + cause.getMessage());
        ctx.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
        if(event instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) event;

            if(e.state() == IdleState.WRITER_IDLE) {
                sendKeepAlive(ctx);
            } else if(e.state() == IdleState.READER_IDLE) {
                clientTimeout(ctx);
            }
        }
    }

    private boolean needAck(Message message) {
        return message.getSequence() == message.getBatch().getBatchSize();
    }

    private void ack(ChannelHandlerContext ctx, Message message) {
        writeAck(ctx, message.getBatch().getProtocol(), message.getSequence());
    }

    private void writeAck(ChannelHandlerContext ctx, byte protocol, int sequence) {
        ctx.write(new Ack(protocol, sequence));
    }

    private void clientTimeout(ChannelHandlerContext ctx) {
        if(!ctx.channel().attr(PROCESSING).get()) {
            logger.debug("Client Timeout: " + ctx.channel().id());
            ctx.close();
        }
    }

    private void sendKeepAlive(ChannelHandlerContext ctx) {
        // If we are actually blocked on processing
        // we can send a keep alive.
        if(ctx.channel().attr(PROCESSING).get()) {
            writeAck(ctx, Protocol.VERSION_2, 0);
        }
    }
}