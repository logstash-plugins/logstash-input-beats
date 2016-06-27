package org.logstash.beats;

import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


// This need to be implemented in Ruby
public class MessageListener implements IMessageListener {
    private final static Logger logger = LogManager.getLogger(MessageListener.class.getName());

    @Override
    public void onNewMessage(ChannelHandlerContext ctx, Message message) {
        logger.debug("onNewMessage");
    }

    @Override
    public void onNewConnection(ChannelHandlerContext ctx) {
        logger.debug("onNewConnection");
    }

    @Override
    public void onConnectionClose(ChannelHandlerContext ctx) {
        logger.debug("onConnectionClose");
    }

    @Override
    public void onException(ChannelHandlerContext ctx) {
        logger.debug("onException");
    }
}
