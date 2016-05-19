package org.logstash.beats;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *  This Class is mostly used in the test suite to make the right assertions with the encoded data frame.
 *  This class support creating v1 or v2 lumberjack frames.
 *
 */
public class AckEncoder extends MessageToByteEncoder<Ack> {
    private final static Logger logger = LogManager.getLogger(AckEncoder.class.getName());


    @Override
    protected void encode(ChannelHandlerContext ctx, Ack ack, ByteBuf out) throws Exception {
        out.writeByte(ack.getProtocol());
        out.writeByte('A');
        out.writeInt(ack.getSequence());
    }
}
