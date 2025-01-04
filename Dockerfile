FROM azul/zulu-openjdk:17

COPY ./target/myapp-1.0-SNAPSHOT.jar app.jar
COPY ./monitoring/jmx_prometheus_javaagent-1.1.0.jar jmx_prometheus_javaagent-1.1.0.jar
COPY ./monitoring/kafka_streams.yml kafka_streams.yml

EXPOSE 9999
CMD ["java", "-javaagent:./jmx_prometheus_javaagent-1.1.0.jar=9999:kafka_streams.yml", "-cp", "app.jar", "com.github.rampi.App"]