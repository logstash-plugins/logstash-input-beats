package org.logstash.beats;

import io.netty.channel.ChannelHandlerContext;

/**
 * Created by ph on 2016-05-18.
 */
public interface IMessageListener {
    public void onNewMessage(ChannelHandlerContext ctx, Message message);
    public void onNewConnection(ChannelHandlerContext ctx);
    public void onConnectionClose(ChannelHandlerContext ctx);
    public void onException(ChannelHandlerContext ctx);
}
