package org.logstash.beats;

import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is implemented in ruby in `lib/logstash/inputs/beats/message_listener`,
 * this class is used to link the events triggered from the different connection to the actual
 * work inside the plugin.
 */
// This need to be implemented in Ruby
public class MessageListener implements IMessageListener {
    private final static Logger logger = LogManager.getLogger(MessageListener.class);


    /**
     * This is triggered on every new message parsed by the beats handler
     * and should be executed in the ruby world.
     *
     * @param ctx
     * @param message
     */
    public void onNewMessage(ChannelHandlerContext ctx, Message message) {
        logger.debug("onNewMessage");
    }

    /**
     * Triggered when a new client connect to the input, this is used to link a connection
     * to a codec in the ruby world.
     * @param ctx
     */
    public void onNewConnection(ChannelHandlerContext ctx) {
        logger.debug("onNewConnection");
    }

    /**
     * Triggered when a connection is close on the remote end and we need to flush buffered
     * events to the queue.
     *
     * @param ctx
     */
    public void onConnectionClose(ChannelHandlerContext ctx) {
        logger.debug("onConnectionClose");
    }

    /**
     * Called went something bad occur in the pipeline, allow to clear buffered codec went
     * somethign goes wrong.
     *
     * @param ctx
     * @param cause
     */
    public void onException(ChannelHandlerContext ctx, Throwable cause) {
        logger.debug("onException");
    }

    /**
     * Called when a error occur in the channel initialize, usually ssl handshake error.
     *
     * @param ctx
     * @param cause
     */
    public void onChannelInitializeException(ChannelHandlerContext ctx, Throwable cause) {
        logger.debug("onException");
    }
}
