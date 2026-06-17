package com.messaging.common.validation;

import com.messaging.common.exception.ErrorCode;
import com.messaging.common.exception.MessagingException;

import java.util.regex.Pattern;

/**
 * Topic-name validation. Topics are used as filesystem path segments
 * ({@code dataDir.resolve(topic)}), so an unvalidated topic from a network client is a path
 * traversal vector. This is the single source of truth for "is this topic name safe".
 *
 * <p>A valid topic starts with an alphanumeric and then contains only {@code [A-Za-z0-9._-]},
 * up to 255 chars, and never contains {@code ".."}. That rejects path separators ({@code /},
 * {@code \}), parent refs ({@code ..}), absolute paths, null bytes, and empty names — while
 * accepting every real topic (e.g. {@code prices-v1}, {@code reference-data-v5}).
 */
public final class TopicNames {

    private static final Pattern VALID = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$");

    private TopicNames() {
    }

    /** @throws MessagingException (VALIDATION_INVALID_ARGUMENT) if the topic is unsafe. */
    public static void validate(String topic) {
        if (topic == null || topic.isEmpty() || topic.contains("..") || !VALID.matcher(topic).matches()) {
            throw new MessagingException(ErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "Invalid topic name: " + (topic == null ? "null" : "'" + topic + "'"))
                    .withContext("topic", String.valueOf(topic));
        }
    }
}
