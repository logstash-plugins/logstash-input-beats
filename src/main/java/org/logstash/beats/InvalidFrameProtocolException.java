package org.logstash.beats;

public class InvalidFrameProtocolException extends RuntimeException {
    InvalidFrameProtocolException(String message) {
        super(message);
    }
}
