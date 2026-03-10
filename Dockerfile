FROM amazoncorretto:17-alpine-jdk

MAINTAINER Technology_Retail_PriceService_UK_5 <Technology_Retail_PriceService_UK_5@ABC.com>

# Install necessary packages
RUN apk add --no-cache ca-certificates bash && update-ca-certificates

# Create group+user (gid=1099, uid=1099)
RUN addgroup -g 1099 app && adduser -D -u 1099 -G app app

# Create application directories
RUN mkdir -p /opt/magicpipe_provider \
             /etc/magicpipe_provider \
             /var/data/magicpipe \
             /var/log/magicpipe_provider

# Set ownership
RUN chown -R 1099:1099 /opt/magicpipe_provider \
                       /etc/magicpipe_provider \
                       /var/data/magicpipe \
                       /var/log/magicpipe_provider

# Copy application JAR (Micronaut runner JAR)
COPY --chown=1099:1099 broker/build/libs/broker-runner.jar /opt/magicpipe_provider/provider-all.jar

# Copy configuration files
COPY --chown=1099:1099 configuration/* /etc/magicpipe_provider/

# Copy docker scripts
COPY --chown=1099:1099 docker/* /opt/magicpipe_provider/

# Make start script executable
RUN chmod +x /opt/magicpipe_provider/start.sh

# Expose ports
# 8080 - Main HTTP API (if needed)
# 8081 - Metrics/Health endpoint
# 5005 - Remote debugging
EXPOSE 8080
EXPOSE 8081
EXPOSE 5005

# Switch to non-root user
USER 1099

# Start the application
ENTRYPOINT ["/bin/bash", "/opt/magicpipe_provider/start.sh"]
