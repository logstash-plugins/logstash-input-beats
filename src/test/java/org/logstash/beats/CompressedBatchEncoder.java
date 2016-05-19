package org.logstash.beats;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class CompressedBatchEncoder extends BatchEncoder {
    private final static Logger logger = LogManager.getLogger(BatchEncoder.class.getName());

    @Override
    protected ByteBuf getPayload(ChannelHandlerContext ctx, Batch batch) throws IOException {
        ByteBuf payload = super.getPayload(ctx, batch);
        try {
            try (ByteBufOutputStream output = new ByteBufOutputStream(ctx.alloc().buffer())) {
                DeflaterOutputStream outputDeflater = new DeflaterOutputStream(output, new Deflater());
                byte[] chunk = new byte[payload.readableBytes()];
                payload.readBytes(chunk);
                outputDeflater.write(chunk);
                outputDeflater.close();
                ByteBuf content = ctx.alloc().buffer(output.writtenBytes());
                content.writeByte(batch.getProtocol());
                content.writeByte('C');

                content.writeInt(output.writtenBytes());
                content.writeBytes(output.buffer());
                return content;
            }
        } finally {
            payload.release();
        }

    }
}