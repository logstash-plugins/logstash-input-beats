package org.logstash.beats;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ph on 2016-06-01.
 */
public class BeatsHandlerTest {
    private SpyListener spyListener;
    private BeatsHandler beatsHandler;
    private Batch batch;

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
            onNewConnectionCalled = true;
        }

        @Override
        public void onConnectionClose(ChannelHandlerContext ctx) {
            onConnectionCloseCalled = true;
        }

        @Override
        public void onException(ChannelHandlerContext ctx) { onExceptionCalled = true; }

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
        beatsHandler = new BeatsHandler(spyListener);

        Message message1 = new Message(1, new HashMap());
        Message message2 = new Message(2, new HashMap());

        batch = new Batch();
        batch.setBatchSize(2);
        batch.setProtocol(Protocol.VERSION_1);
        batch.addMessage(message1);
        batch.addMessage(message2);

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


        assertEquals(2, spyListener.getLastMessages().size());
        embeddedChannel.close();
    }

    @Test
    public void TestItAckLastMessageFromBatch() {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new BeatsHandler(spyListener));
        embeddedChannel.writeInbound(batch);


        embeddedChannel.close();
    }
}
