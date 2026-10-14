# Java Client (bot) - Parte 1
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /build/target/vetflow-client.jar ./vetflow-client.jar

# ACTION, BOT_NAME, SERVER_ADDRESS, CHANNEL_NAME sao definidos no
# docker-compose.yml por servico (cada bot faz UMA acao e sai).
CMD ["java", "-jar", "vetflow-client.jar"]