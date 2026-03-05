package com.messaging.common.http;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.ClientFilterChain;
import io.micronaut.http.filter.HttpClientFilter;
import jakarta.inject.Singleton;
import org.reactivestreams.Publisher;

/**
 * Adds a configured auth token header to all outbound Micronaut HTTP client calls.
 */
@Singleton
@Filter("/**")
@Requires(property = "broker.http.auth.token")
public class AuthTokenClientFilter implements HttpClientFilter {
    private final String headerName;
    private final String headerValue;

    public AuthTokenClientFilter(
            @Value("${broker.http.auth.token}") String token,
            @Value("${broker.http.auth.header:Authorization}") String headerName,
            @Value("${broker.http.auth.scheme:Bearer}") String scheme) {
        this.headerName = headerName != null && !headerName.isBlank() ? headerName : "Authorization";
        String trimmedToken = token != null ? token.trim() : "";
        String trimmedScheme = scheme != null ? scheme.trim() : "";
        if (trimmedScheme.isEmpty()) {
            this.headerValue = trimmedToken;
        } else {
            this.headerValue = trimmedScheme + " " + trimmedToken;
        }
    }

    @Override
    public Publisher<? extends io.micronaut.http.HttpResponse<?>> doFilter(
            MutableHttpRequest<?> request,
            ClientFilterChain chain) {
        if (headerValue != null && !headerValue.isBlank()) {
            request.header(headerName, headerValue);
        }
        return chain.proceed(request);
    }
}
