FROM maven:3.9.6-eclipse-temurin-17 as BUILD_IMAGE
ENV APP_HOME=/root/dev/myapp/
RUN mkdir -p $APP_HOME/src/main/java
WORKDIR $APP_HOME
COPY . .
RUN ls -alh
RUN mvn package

FROM eclipse-temurin:17-ubi9-minimal
LABEL authors="ram-pi"
COPY --from=BUILD_IMAGE /root/dev/myapp/target/myapp-1.0-SNAPSHOT.jar app.jar
COPY ./monitoring/jmx_prometheus_javaagent-1.1.0.jar jmx_prometheus_javaagent-1.1.0.jar
COPY ./monitoring/kafka_streams.yml kafka_streams.yml

EXPOSE 9999
CMD ["java", "-javaagent:./jmx_prometheus_javaagent-1.1.0.jar=9999:kafka_streams.yml", "-cp", "app.jar", "com.github.rampi.MyAppWithCustomRetention"]