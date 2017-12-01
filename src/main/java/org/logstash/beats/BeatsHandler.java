package org.logstash.beats;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLHandshakeException;

public class BeatsHandler extends SimpleChannelInboundHandler<Batch> {
    private final static Logger logger = LogManager.getLogger(BeatsHandler.class);
    private final AtomicBoolean processing = new AtomicBoolean(false);
    private final IMessageListener messageListener;
    private ChannelHandlerContext context;


    public BeatsHandler(IMessageListener listener) {
        messageListener = listener;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        context = ctx;
        messageListener.onNewConnection(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        messageListener.onConnectionClose(ctx);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Batch batch) throws Exception {
        if(logger.isDebugEnabled()) {
            logger.debug(format("Received a new payload"));
        }

        processing.compareAndSet(false, true);

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
        processing.compareAndSet(true, false);

    }

    /*
     * Do not propagate the ssl handshake exception down to the ruby layer
     * handle it locally and close the connection if the channel is still active.
     * Calling `onException` will flush the content of the codec's buffer, this call may block the thread
     * in the event loop until completion, this should only affect LS 5 because it still supporting
     * the multiline codec, v6 drop support for buffering codec in the input.
     *
     * For v5, I cannot simply drop the content of the buffer because this will create data loss, because multiline
     * content can overlap Filebeat transmission windows, we were recommending multiline at the source in v5 and in v6
     * we enforce it.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();

        if (!(cause instanceof SSLHandshakeException)) {
            messageListener.onException(ctx, cause);
        }

        logger.info(format("Exception: " + cause.getMessage()));
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
        if(event instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) event;

            if(e.state() == IdleState.WRITER_IDLE) {
                sendKeepAlive();
            } else if(e.state() == IdleState.ALL_IDLE) {
                clientTimeout();
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

    private void clientTimeout() {
        if(!processing.get()) {
            if(logger.isDebugEnabled()) {
                logger.debug(format("Client Timeout"));
            }
            this.context.close();
        }
    }

    private void sendKeepAlive() {
        // If we are actually blocked on processing
        // we can send a keep alive.
        if(processing.get()) {
            if(logger.isDebugEnabled()) {
                logger.debug(format("Still processing event current batch, sending keep alive"));
            }
            writeAck(context, Protocol.VERSION_2, 0);
        }
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
