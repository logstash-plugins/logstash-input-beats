package org.logstash.beats;

import java.util.ArrayList;
import java.util.List;

public class Batch {
    private byte protocol = Protocol.VERSION_2;
    private int batchSize;
    private List<Message> messages = new ArrayList();

    public List<Message> getMessages() {
        return messages;
    }

    public void addMessage(Message message) {
        message.setBatch(this);
        messages.add(message);
    }

    public int size() {
        return messages.size();
    }

    public void setBatchSize(int size) {
        batchSize = size;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isEmpty() {
        if(0 == messages.size()) {
            return true;
        } else {
            return false;
        }
    }

    public byte getProtocol() {
        return protocol;
    }

    public void setProtocol(byte protocol) {
        protocol = protocol;
    }
}