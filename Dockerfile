FROM java:8

ADD target/neo4j-twitter-load-1.0-SNAPSHOT-jar-with-dependencies.jar /app.jar

CMD ["sh", "-c", "java -jar /app.jar ${CONCURRENCY} ${NEO4J_BOLT_URL} ${MAX_OPERATIONS}"]
