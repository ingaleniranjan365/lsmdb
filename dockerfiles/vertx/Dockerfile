FROM openjdk:17-oracle

WORKDIR /app

COPY com.lsmdb-vertx-fat.jar /app/com.lsmdb-vertx-fat.jar

#ENV JAVA_OPTS="-Xms128m -Xmx256m"
ENV JAVA_OPTS="-Xms256m -Xmx768m"

EXPOSE 8081

CMD ["java", "-jar", "com.lsmdb-vertx-fat.jar"]
