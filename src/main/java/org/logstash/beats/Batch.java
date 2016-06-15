package org.logstash.beats;

import java.util.ArrayList;
import java.util.List;

public class Batch {
    private byte protocol = Protocol.VERSION_2;
    private int windowSize;
    private List<Message> messages = new ArrayList();

    public List<Message> getMessages() {
        return this.messages;
    }

    public void addMessage(Message message) {
        message.setBatch(this);
        this.messages.add(message);
    }

    public int size() {
        return this.messages.size();
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public int getWindowSize() {
        return this.windowSize;
    }

    public boolean isEmpty() {
        if(0 == this.messages.size()) {
            return true;
        } else {
            return false;
        }
    }

    public byte getProtocol() {
        return protocol;
    }

    public void setProtocol(byte protocol) {
        this.protocol = protocol;
    }
}