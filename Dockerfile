# Stage 1: Build
FROM maven:3.8.5-openjdk-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/kafka-sse-service-1.0.0.jar app.jar

ENV KAFKA_BROKERS="localhost:9092"
EXPOSE 8080

ENTRYPOINT ["java", "-Xmx512m", "-Xms512m", "-jar", "app.jar"]
