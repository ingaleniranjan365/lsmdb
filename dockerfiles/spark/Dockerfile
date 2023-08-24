FROM openjdk:17-oracle

WORKDIR /app

COPY com.lsmdb-spark-fat.jar /app/com.lsmdb-spark-fat.jar

#ENV JAVA_OPTS="-Xms128m -Xmx256m"
ENV JAVA_OPTS="-Xms256m -Xmx768m"

EXPOSE 8080

CMD ["java", "-jar", "com.lsmdb-spark-fat.jar"]