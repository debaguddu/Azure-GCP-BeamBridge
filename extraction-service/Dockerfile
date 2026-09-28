# ==============================================================
# Multi-Stage Build: Project Kalinga Extraction Service
# ==============================================================

# Stage 1: Build stage
FROM maven:3.9.9-eclipse-temurin-21-alpine AS builder
WORKDIR /workspace

# Cache dependencies
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Compile and package executable jar
COPY src ./src
RUN mvn clean package -DskipTests -B

# Stage 2: Minimal Distroless / Alpine Runtime
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Create unprivileged system user for container security
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

# Copy built artifact from builder stage
COPY --from=builder /workspace/target/*.jar app.jar

# Expose Actuator and API port
EXPOSE 8080

# Production JVM flags: Bounded memory percentages and G1 Garbage Collection
ENV JAVA_OPTS="-XX:+UseG1GC -XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0 -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
