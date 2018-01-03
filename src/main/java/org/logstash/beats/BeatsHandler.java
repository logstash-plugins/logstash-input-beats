package org.logstash.beats;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLHandshakeException;

public class BeatsHandler extends SimpleChannelInboundHandler<Batch> {
    private final static Logger logger = LogManager.getLogger(BeatsHandler.class);
    public static AttributeKey<Boolean> PROCESSING_BATCH = AttributeKey.valueOf("processing-batch");
    private final IMessageListener messageListener;
    private ChannelHandlerContext context;



    public BeatsHandler(IMessageListener listener) {
        messageListener = listener;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        context = ctx;
        messageListener.onNewConnection(ctx);
        // Give some breathing room on new clients to receive the keep alive.
        ctx.channel().attr(PROCESSING_BATCH).set(false);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(PROCESSING_BATCH).set(false);
        messageListener.onConnectionClose(ctx);
    }


    @Override
    public void channelRead0(ChannelHandlerContext ctx, Batch batch) throws Exception {
        if(logger.isDebugEnabled()) {
            logger.debug(format("Received a new payload"));
        }

        ctx.channel().attr(PROCESSING_BATCH).set(true);
        for(Message message : batch.getMessages()) {
            if(logger.isDebugEnabled()) {
                logger.debug(format("Sending a new message for the listener, sequence: " + message.getSequence()));
            }
            messageListener.onNewMessage(ctx, message);

            if(needAck(message)) {
                ack(ctx, message);
            }
        }
        ctx.flush();
        ctx.channel().attr(PROCESSING_BATCH).set(false);
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();

        if (!(cause instanceof SSLHandshakeException)) {
            messageListener.onException(ctx, cause);
        }

        String causeMessage = cause.getMessage() == null ? cause.getClass().toString() : cause.getMessage();

        if (logger.isDebugEnabled()){
            logger.debug(format("Handling exception: " + causeMessage), cause);
        }
        logger.info(format("Handling exception: " + causeMessage));
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

    /*
     * There is no easy way in Netty to support MDC directly,
     * we will use similar logic than Netty's LoggingHandler
     */
    private String format(String message) {
        InetSocketAddress local = (InetSocketAddress) context.channel().localAddress();
        InetSocketAddress remote = (InetSocketAddress) context.channel().remoteAddress();

        String localhost;
        if(local != null) {
            localhost = local.getAddress().getHostAddress() + ":" + local.getPort();
        } else{
            localhost = "undefined";
        }

        String remotehost;
        if(remote != null) {
            remotehost = remote.getAddress().getHostAddress() + ":" + remote.getPort();
        } else{
            remotehost = "undefined";
        };

        return "[local: " + localhost + ", remote: " + remotehost + "] " + message;
    }
}
