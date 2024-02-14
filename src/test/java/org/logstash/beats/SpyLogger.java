package org.logstash.beats;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.util.MessageSupplier;
import org.apache.logging.log4j.util.Supplier;

import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

final class SpyLogger extends org.apache.logging.log4j.core.Logger {
    // store last message received by the spy
    private String loggedMessage;
    // store last received level
    private Level loggedLevel;

    public SpyLogger(Logger sample) {
        super(new LoggerContext("spylogger"), sample.getName(), sample.getMessageFactory());
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker,
                             final MessageSupplier messageSupplier, final Throwable throwable) {
        loggedMessage = messageSupplier.get().toString();
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final Object message,
                             final Throwable throwable) {
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final Supplier<?> messageSupplier,
                             final Throwable throwable) {
        loggedMessage = messageSupplier.get().toString();
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Supplier<?>... paramSuppliers) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Object... params) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Object p0, final Object p1, final Object p2, final Object p3, final Object p4, final Object p5) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Object p0, final Object p1, final Object p2, final Object p3, final Object p4, final Object p5,
                             final Object p6) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Object p0, final Object p1, final Object p2, final Object p3, final Object p4, final Object p5,
                             final Object p6, final Object p7) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Object p0, final Object p1, final Object p2, final Object p3, final Object p4, final Object p5,
                             final Object p6, final Object p7, final Object p8) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Object p0, final Object p1, final Object p2, final Object p3, final Object p4, final Object p5,
                             final Object p6, final Object p7, final Object p8, final Object p9) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Throwable throwable) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Object p0, final Object p1, final Object p2, final Object p3, final Object p4) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Object p0, final Object p1, final Object p2, final Object p3) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Object p0, final Object p1, final Object p2) {
        loggedMessage = message;
        loggedLevel = level;
    }

    @Override
    public void logIfEnabled(final String fqcn, final Level level, final Marker marker, final String message,
                             final Object p0, final Object p1) {
        loggedMessage = message;
        loggedLevel = level;
    }

    void verifyLogMessage(String assertionMessage, String expectedMessage) {
        assertEquals(assertionMessage, expectedMessage, loggedMessage);
        loggedMessage = null;
    }

    void verifyLevel(Level expectedLevel) {
        assertEquals(expectedLevel, loggedLevel);
        loggedLevel = null;
    }

    void verifyNoLog(String reason) {
        assertThat(reason, loggedMessage, isEmptyOrNullString());
    }
}
