package org.logstash.beats;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.concurrent.atomic.AtomicBoolean;

@ChannelHandler.Sharable
public class BeatsHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = LogManager.getLogger(BeatsHandler.class.getName());
    private final AtomicBoolean processing = new AtomicBoolean(false);
    private final IMessageListener messageListener;
    private ChannelHandlerContext ctx;


    public BeatsHandler(IMessageListener messageListener) {
        this.messageListener = messageListener;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        this.messageListener.onNewConnection(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        this.messageListener.onConnectionClose(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) {
        logger.debug("Received a new payload");

        this.processing.compareAndSet(false, true);

        Batch batch = (Batch) data;
        for(Message message : batch.getMessages()) {
            logger.debug("Sending a new message for the listener, sequence: " + message.getSequence());
            this.messageListener.onNewMessage(ctx, message);

            if(needAck(message)) {
                ack(ctx, message);
            }
        }

        this.processing.compareAndSet(true, false);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        this.messageListener.onException(ctx);

        logger.error("Exception", cause);
        ctx.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
        if(event instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) event;

            if(e.state() == IdleState.WRITER_IDLE) {
                this.sendKeepAlive();
            } else if(e.state() == IdleState.READER_IDLE) {
                this.clientTimeout();
            }
        }
    }

    private boolean needAck(Message message) {
        if (message.getSequence() == message.getBatch().getWindowSize()) {
            return true;
        } else {
            return false;
        }
    }

    private void ack(ChannelHandlerContext ctx, Message message) {
        writeAck(ctx, message.getBatch().getProtocol(), message.getSequence());
    }

    public void writeAck(ChannelHandlerContext ctx, byte protocol, int sequence) {
        ByteBuf buffer = ctx.alloc().buffer(6);
        buffer.writeByte(protocol);
        buffer.writeByte('A');
        buffer.writeInt(sequence);
        ctx.writeAndFlush(buffer);
    }

    private void clientTimeout() {
        logger.debug("Client Timeout");
        this.ctx.close();
    }

    private void sendKeepAlive() {
        // If we are actually blocked on processing
        // we can send a keep alive.
        if(this.processing.get()) {
            writeAck(this.ctx, Protocol.VERSION_2, 0);
        }
    }
}