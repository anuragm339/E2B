package com.messaging.broker.monitoring;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns a recorded error into a human "what happened / how it surfaced / why" triple for the
 * self-status error API, so a row reads as plain English instead of a stack-of-class-names. Pure
 * string mapping — no I/O, no state.
 */
public final class ErrorExplainer {

    private ErrorExplainer() {
    }

    private static final Pattern CATEGORY = Pattern.compile("category=([A-Z_]+)");
    private static final Pattern RETRIABLE = Pattern.compile("retriable=(true|false)");
    private static final Pattern LEADING_CODE = Pattern.compile("^\\[([A-Z][A-Z0-9_]{2,})\\]");

    /**
     * WHAT failed: the {@code ErrorCode} if known, else a {@code [CODE]} embedded in the message
     * (the code IS the identity; its params go into {@code why}), else the human message, else type.
     */
    public static String what(String errorCode, String exceptionClass, String message) {
        if (errorCode != null) return errorCode;
        if (message != null && !message.isBlank()) {
            Matcher code = LEADING_CODE.matcher(message);
            if (code.find()) return code.group(1);
            String m = message.trim();
            int nl = m.indexOf('\n');
            if (nl > 0) m = m.substring(0, nl).trim();
            if (!m.isEmpty()) {
                return m.length() > 200 ? m.substring(0, 200) : m;
            }
        }
        return simple(exceptionClass);
    }

    /** HOW it surfaced: the component that logged it and the exception type that carried it. */
    public static String how(String logger, String exceptionClass) {
        String comp = simple(logger);
        if (exceptionClass != null) {
            return "surfaced as " + simple(exceptionClass) + (comp != null ? " in " + comp : "");
        }
        return comp != null ? "logged by " + comp : "logged event";
    }

    /** WHY it happened: ErrorCode metadata (category/retriable) plus a known cause for the exception. */
    public static String why(String errorCode, String exceptionClass, String message) {
        StringBuilder sb = new StringBuilder();
        if (errorCode != null) sb.append("errorCode=").append(errorCode);
        String cat = group(CATEGORY, message);
        String retr = group(RETRIABLE, message);
        if (cat != null) sb.append(sb.length() > 0 ? ", " : "").append("category=").append(cat);
        if (retr != null) sb.append(sb.length() > 0 ? ", " : "").append("retriable=").append(retr);
        String cause = causeOf(exceptionClass);
        if (cause != null) sb.append(sb.length() > 0 ? " — " : "").append(cause);
        return sb.length() > 0 ? sb.toString() : "see message";
    }

    private static String causeOf(String exceptionClass) {
        if (exceptionClass == null) return null;
        String s = exceptionClass;
        if (s.endsWith("UnknownHostException")) return "host name could not be resolved (DNS); the upstream is unreachable";
        if (s.endsWith("NoRouteToHostException")) return "no network route to the host";
        if (s.endsWith("ConnectTimeoutException")) return "connection could not be established within the timeout";
        if (s.endsWith("ConnectException")) return "connection was refused by the host";
        if (s.contains("ReadTimeout") || s.endsWith("SocketTimeoutException") || s.endsWith("TimeoutException"))
            return "the upstream did not respond within the timeout";
        if (s.endsWith("HttpClientException")) return "the HTTP call to the upstream failed";
        if (s.endsWith("ClosedChannelException")) return "the connection was closed mid-transfer";
        if (s.endsWith("IOException")) return "a low-level I/O error occurred";
        return null;
    }

    private static String group(Pattern p, String s) {
        if (s == null) return null;
        Matcher m = p.matcher(s);
        return m.find() ? m.group(1) : null;
    }

    /** Simple name: last dot segment, then last {@code $} segment (drops package + outer class). */
    private static String simple(String fqcn) {
        if (fqcn == null) return null;
        int dot = fqcn.lastIndexOf('.');
        String s = dot >= 0 ? fqcn.substring(dot + 1) : fqcn;
        int dollar = s.lastIndexOf('$');
        return dollar >= 0 ? s.substring(dollar + 1) : s;
    }
}
