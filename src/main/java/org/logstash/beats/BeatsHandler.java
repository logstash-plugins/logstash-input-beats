package org.logstash.beats;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import javax.net.ssl.SSLHandshakeException;

public class BeatsHandler extends SimpleChannelInboundHandler<Batch> {

    private final static Logger LOGGER = LogManager.getLogger(BeatsHandler.class);

    private final IMessageListener messageListener;

    private ChannelHandlerContext context;

    private static final String CONNECTION_RESET = "Connection reset by peer";

    public BeatsHandler(IMessageListener listener) {
        messageListener = listener;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        context = ctx;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Channel Active"));
        }
        super.channelActive(ctx);
        messageListener.onNewConnection(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Channel Inactive"));
        }
        messageListener.onConnectionClose(ctx);
    }


    @Override
    public void channelRead0(ChannelHandlerContext ctx, Batch batch) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Received a new payload"));
        }
        try {
            if (batch.isEmpty()) {
                LOGGER.debug("Sending 0-seq ACK for empty batch");
                writeAck(ctx, batch.getProtocol(), 0);
            }
            for (Message message : batch) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(format("Sending a new message for the listener, sequence: " + message.getSequence()));
                }
                messageListener.onNewMessage(ctx, message);

                if (needAck(message)) {
                    ack(ctx, message);
                }
            }
        } finally {
            //this channel is done processing this payload, instruct the connection handler to stop sending TCP keep alive
            ctx.channel().attr(ConnectionHandler.CHANNEL_SEND_KEEP_ALIVE).get().set(false);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{}: batches pending: {}", ctx.channel().id().asShortText(), ctx.channel().attr(ConnectionHandler.CHANNEL_SEND_KEEP_ALIVE).get().get());
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
            if (!(cause instanceof SSLHandshakeException)) {
                messageListener.onException(ctx, cause);
            }
            if (isNoisyException(cause)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.info(format("closing"), cause);
                } else {
                    LOGGER.info(format("closing (" + cause.getMessage() + ")"));
                }
            } else {
                final Throwable realCause = extractCause(cause, 0);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.info(format("Handling exception: " + cause + " (caused by: " + realCause + ")"), cause);
                } else {
                    LOGGER.info(format("Handling exception: " + cause + " (caused by: " + realCause + ")"));
                }
                super.exceptionCaught(ctx, cause);
            }
        } finally {
            ctx.flush();
            ctx.close();
        }
    }

    private boolean isNoisyException(final Throwable ex) {
        if (ex instanceof IOException) {
            final String message = ex.getMessage();
            return CONNECTION_RESET.equals(message);
        }
        return false;
    }

    private boolean needAck(Message message) {
        return message.getSequence() == message.getBatch().getHighestSequence();
    }

    private void ack(ChannelHandlerContext ctx, Message message) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Acking message number " + message.getSequence()));
        }
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

        final String localhost = Objects.isNull(local)
                ? "undefined"
                : local.getAddress().getHostAddress() + ":" + local.getPort();
        final String remoteHost = Objects.isNull(remote)
                ? "undefined"
                : remote.getAddress().getHostAddress() + ":" + remote.getPort();
        return "[local: " + localhost + ", remote: " + remoteHost + "] " + message;
    }

    private static final int MAX_CAUSE_NESTING = 10;

    private static Throwable extractCause(final Throwable ex, final int nesting) {
        final Throwable cause = ex.getCause();
        if (cause == null || cause == ex) return ex;
        if (nesting >= MAX_CAUSE_NESTING) return cause; // do not recurse infinitely
        return extractCause(cause, nesting + 1);
    }
}
