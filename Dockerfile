# Multi-stage Dockerfile for Messaging Provider Broker

# Stage 1: Build
FROM gradle:8.5-jdk17 AS builder

WORKDIR /build

# Copy CA certificate and import it into JDK trust store
# This is required for Maven Central access during Gradle build
COPY mvnrepository.com.pem /tmp/mvnrepository.com.pem
RUN keytool -import -trustcacerts -noprompt \
    -alias messaging_maven_ca \
    -file /tmp/mvnrepository.com.pem \
    -keystore /opt/java/openjdk/lib/security/cacerts \
    -storepass changeit && \
    rm /tmp/mvnrepository.com.pem

# Copy gradle files first for better caching
COPY settings.gradle build.gradle ./
COPY gradle gradle/
COPY gradlew ./

# Copy all module sources
COPY common/ common/
COPY storage/ storage/
COPY network/ network/
COPY pipe/ pipe/
COPY broker/ broker/

# Build all modules (now with CA certificate trusted)
RUN ./gradlew :broker:build -x test --no-daemon

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-jammy

# Create non-root user
RUN groupadd -r broker && useradd -r -g broker broker

# Install SQLite and curl for healthcheck
RUN apt-get update && \
    apt-get install -y sqlite3 libsqlite3-dev curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Extract the broker distribution (contains all libs and scripts)
COPY --from=builder /build/broker/build/distributions/broker.tar /tmp/broker.tar
RUN tar -xf /tmp/broker.tar -C /app --strip-components=1 && rm /tmp/broker.tar

# Create data directory with proper permissions
RUN mkdir -p /app/data && chown -R broker:broker /app

# Switch to non-root user
USER broker

# Environment variables with defaults
ENV NODE_ID=broker-001 \
    BROKER_PORT=9092 \
    HTTP_PORT=8081 \
    DATA_DIR=/app/data \
    REGISTRY_URL=http://localhost:8080 \
    JAVA_OPTS="-Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Expose ports
EXPOSE 9092 8081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${HTTP_PORT}/health || exit 1

# Run the application using the distribution script
CMD ["/app/bin/broker"]
