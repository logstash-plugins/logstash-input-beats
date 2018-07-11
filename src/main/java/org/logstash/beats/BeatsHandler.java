package org.logstash.beats;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import javax.net.ssl.SSLHandshakeException;

public class BeatsHandler extends SimpleChannelInboundHandler<Batch> {
    private final static Logger logger = LogManager.getLogger(BeatsHandler.class);
    private final IMessageListener messageListener;

    public BeatsHandler(IMessageListener listener) {
        messageListener = listener;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        if (logger.isTraceEnabled()){
            logger.trace("Channel Active");
        }
        super.channelActive(ctx);
        messageListener.onNewConnection(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        if (logger.isTraceEnabled()){
            logger.trace("Channel Inactive");
        }
  
        messageListener.onConnectionClose(ctx);
    }


    @Override
    public void channelRead0(ChannelHandlerContext ctx, Batch batch) throws Exception {
        if(logger.isDebugEnabled()) {
            logger.debug("Received a new payload");
        }
        try {
            for (Message message : batch) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sending a new message for the listener, sequence: " + message.getSequence());
                }
                messageListener.onNewMessage(ctx, message);

                if (needAck(message)) {
                    ack(ctx, message);
                }
            }
        }finally{
            //this channel is done processing this payload, instruct the connection handler to stop sending TCP keep alive
            ctx.channel().attr(ConnectionHandler.CHANNEL_SEND_KEEP_ALIVE).get().set(false);
            if (logger.isDebugEnabled()) {
                logger.debug("{}: batches pending: {}", ctx.channel().id().asShortText(),ctx.channel().attr(ConnectionHandler.CHANNEL_SEND_KEEP_ALIVE).get().get());
            }
            batch.release();
            ctx.flush();
        }
    }

    /*
     * Do not propagate the SSL handshake exception down to the ruby layer handle it locally instead and close the connection
     * if the channel is still active. Calling `onException` will flush the content of the codec's buffer, this call
     * may block the thread in the event loop until completion, this should only affect LS 5 because it still supports
     * the multiline codec, v6 drop support for buffering codec in the beats input.
     *
     * For v5, I cannot drop the content of the buffer because this will create data loss because multiline content can
     * overlap Filebeat transmission; we were recommending multiline at the source in v5 and in v6 we enforce it.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        try {
            InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();

            String causeMessage = cause.getMessage() == null ? cause.getClass().toString() : cause.getMessage();

            if (remoteAddress != null) {
                logger.info("Exception: " + causeMessage + ", from: " + remoteAddress.toString());
            } else {
                logger.info("Exception: " + causeMessage);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Exception: " + causeMessage, cause);
            }

            if (!(cause instanceof SSLHandshakeException)) {
                messageListener.onException(ctx, cause);
            }
        }finally{
            super.exceptionCaught(ctx, cause);
            ctx.flush();
            ctx.close();
        }

    }

    private boolean needAck(Message message) {
        return message.getSequence() == message.getBatch().getHighestSequence();
    }

    private void ack(ChannelHandlerContext ctx, Message message) {
        writeAck(ctx, message.getBatch().getProtocol(), message.getSequence());
    }

    private void writeAck(ChannelHandlerContext ctx, byte protocol, int sequence) {
        ctx.write(new Ack(protocol, sequence));
    }
}
