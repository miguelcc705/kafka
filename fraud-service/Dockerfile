# syntax=docker/dockerfile:1
# Contexto de build: raíz del repositorio (docker compose lo configura así).

FROM eclipse-temurin:21.0.13_9-jdk-noble AS build
WORKDIR /workspace

# Primero los descriptores de build, para aprovechar la caché de capas.
COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY transfer-service/pom.xml transfer-service/pom.xml
COPY fraud-service/pom.xml fraud-service/pom.xml
COPY fraud-service/src fraud-service/src

RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -q -B -pl fraud-service -am -DskipTests package

FROM eclipse-temurin:21.0.13_9-jre-noble
WORKDIR /app

RUN groupadd --system app && useradd --system --gid app --no-create-home app

COPY --from=build /workspace/fraud-service/target/fraud-service.jar app.jar

USER app
EXPOSE 8081
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
