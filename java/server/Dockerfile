# Java Server - Parte 1
# Multi-stage: compila com Maven (imagem grande, so usada no build),
# depois roda o jar ja compilado numa imagem final bem menor (so JRE).

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
# Baixa as dependencias antes de copiar o codigo-fonte, para o cache de
# camadas do Docker nao invalidar o download sempre que um .java mudar.
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /build/target/vetflow-server.jar ./vetflow-server.jar

EXPOSE 5555
VOLUME ["/app/data"]

CMD ["java", "-jar", "vetflow-server.jar"]