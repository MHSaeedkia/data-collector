FROM apache/nifi:2.9.0

# PostgreSQL JDBC driver — required by DBCPConnectionPool controller service
# Set "Database Driver Location(s)" to: /opt/nifi/nifi-current/drivers/postgresql-42.7.11.jar
# Source processor: QueryDatabaseTable / ExecuteSQL
# Sink processor:   PutDatabaseRecord / ExecuteSQL
RUN mkdir -p /opt/nifi/nifi-current/drivers \
   && curl -sL -o /opt/nifi/nifi-current/drivers/postgresql-42.7.11.jar \
   https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.11/postgresql-42.7.11.jar


# Ensure OOM crashes the JVM cleanly instead of leaving it half-dead
RUN echo "java.arg.100=-XX:+ExitOnOutOfMemoryError" >> /opt/nifi/nifi-current/conf/bootstrap.conf

