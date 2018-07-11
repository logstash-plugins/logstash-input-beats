package org.logstash.beats;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ph on 2016-06-01.
 */
public class BeatsHandlerTest {
    private static final SecureRandom randomizer = new SecureRandom();
    private SpyListener spyListener;
    private int startSequenceNumber = randomizer.nextInt(100);
    private int messageCount = 5;
    private V1Batch batch;

    private class SpyListener implements IMessageListener {
        private boolean onNewConnectionCalled = false;
        private boolean onNewMessageCalled = false;
        private boolean onConnectionCloseCalled = false;
        private boolean onExceptionCalled = false;
        private final List<Message> lastMessages = new ArrayList<Message>();

        @Override
        public void onNewMessage(ChannelHandlerContext ctx, Message message) {
            onNewMessageCalled = true;
            lastMessages.add(message);
        }

        @Override
        public void onNewConnection(ChannelHandlerContext ctx) {
            ctx.channel().attr(ConnectionHandler.CHANNEL_SEND_KEEP_ALIVE).set(new AtomicBoolean(false));
            onNewConnectionCalled = true;
        }

        @Override
        public void onConnectionClose(ChannelHandlerContext ctx) {
            onConnectionCloseCalled = true;
        }

        @Override
        public void onException(ChannelHandlerContext ctx, Throwable cause) { onExceptionCalled = true; }

        @Override
        public void onChannelInitializeException(ChannelHandlerContext ctx, Throwable cause) {
        }

        public boolean isOnNewConnectionCalled() {
            return onNewConnectionCalled;
        }

        public boolean isOnNewMessageCalled() {
            return onNewMessageCalled;
        }

        public boolean isOnConnectionCloseCalled() {
            return onConnectionCloseCalled;
        }

        public List<Message> getLastMessages() {
            return lastMessages;
        }

        public boolean isOnExceptionCalled() {
            return onExceptionCalled;
        }
    }

    @Before
    public void setup() {
        spyListener = new SpyListener();
        batch = new V1Batch();
        batch.setBatchSize(messageCount);
        for (int i = 0;i < messageCount;i++) {
            Message message = new Message(i + startSequenceNumber, new HashMap());
            batch.addMessage(message);
        }
    }

    @Test
    public void TestItCalledOnNewConnectionOnListenerWhenHandlerIsAdded() {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new BeatsHandler(spyListener));
        embeddedChannel.writeInbound(batch);

        assertTrue(spyListener.isOnNewConnectionCalled());
        embeddedChannel.close();
    }

    @Test
    public void TestItCalledOnConnectionCloseOnListenerWhenChannelIsRemoved() {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new BeatsHandler(spyListener));
        embeddedChannel.writeInbound(batch);
        embeddedChannel.close();

        assertTrue(spyListener.isOnConnectionCloseCalled());
    }

    @Test
    public void TestIsCallingNewMessageOnEveryMessage() {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new BeatsHandler(spyListener));
        embeddedChannel.writeInbound(batch);


        assertEquals(messageCount, spyListener.getLastMessages().size());
        embeddedChannel.close();
    }

    @Test
    public void testAcksLastMessageInBatch() {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new BeatsHandler(spyListener));
        embeddedChannel.writeInbound(batch);
        assertEquals(messageCount, spyListener.getLastMessages().size());
        Ack ack = embeddedChannel.readOutbound();
        assertEquals(ack.getProtocol(), Protocol.VERSION_1);
        assertEquals(ack.getSequence(), startSequenceNumber + messageCount - 1);
        embeddedChannel.close();
    }
}
