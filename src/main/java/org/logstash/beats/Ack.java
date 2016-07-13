package org.logstash.beats;

public class Ack {
    private final byte protocol;
    private final int sequence;
    public Ack(byte protocol, int sequence) {
        this.protocol = protocol;
        this.sequence = sequence;
    }

    public byte getProtocol() {
        return protocol;
    }

    public int getSequence() {
        return sequence;
    }
}
