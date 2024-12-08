# Use MySQL official image as base
FROM mysql:8.0

# Set environment variables
ENV MYSQL_ROOT_PASSWORD=rootpassword
ENV MYSQL_DATABASE=IndexPerformanceTest

# Copy initialization script
COPY init.sql /docker-entrypoint-initdb.d/init.sql

# Expose MySQL port
EXPOSE 3306
