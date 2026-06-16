package com.messaging.broker.monitoring;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.AppenderBase;
import com.messaging.common.exception.MessagingException;

import java.util.Map;

/**
 * Logback appender that mirrors WARN/ERROR log events into the in-memory {@link ErrorRecorder}
 * so the self-status API can serve them as JSON (no log-grep). Universal — it captures every
 * WARN/ERROR, and when the throwable is (or wraps) a {@link MessagingException} it enriches the
 * entry with the {@code ErrorCode} and context.
 *
 * <p>Declared in {@code logback.xml} and instantiated by logback (no-arg), so it reaches the
 * DI-managed {@link ErrorRecorder} through that bean's static bridge.
 */
public class StatusErrorAppender extends AppenderBase<ILoggingEvent> {

    @Override
    protected void append(ILoggingEvent event) {
        try {
            ErrorRecorder recorder = ErrorRecorder.instance();
            if (recorder == null) {
                return; // DI not up yet — drop early startup events
            }
            if (!event.getLevel().isGreaterOrEqual(Level.WARN)) {
                return;
            }

            String errorCode = null;
            String exceptionClass = null;
            Map<String, Object> context = null;

            IThrowableProxy tp = event.getThrowableProxy();
            if (tp != null) {
                exceptionClass = tp.getClassName();
                // ThrowableProxy (the concrete type) exposes the original Throwable, so we can
                // recover the MessagingException's ErrorCode/context — IThrowableProxy alone cannot.
                if (tp instanceof ThrowableProxy realProxy) {
                    MessagingException me = findMessagingException(realProxy.getThrowable());
                    if (me != null) {
                        if (me.getErrorCode() != null) {
                            errorCode = me.getErrorCode().name();
                        }
                        Map<String, Object> ctx = me.getContext();
                        if (ctx != null && !ctx.isEmpty()) {
                            context = ctx;
                        }
                        exceptionClass = me.getClass().getName();
                    }
                }
            }

            recorder.record(event.getTimeStamp(), event.getLevel().toString(),
                    event.getLoggerName(), event.getFormattedMessage(),
                    errorCode, exceptionClass, context);
        } catch (Exception ignored) {
            // The recorder must never disturb the logging path.
        }
    }

    private static MessagingException findMessagingException(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof MessagingException me) {
                return me;
            }
        }
        return null;
    }
}
