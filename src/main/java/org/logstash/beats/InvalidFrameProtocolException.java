package org.logstash.beats;

public class InvalidFrameProtocolException extends Exception {
    InvalidFrameProtocolException(String message) {
        super(message);
    }
}
