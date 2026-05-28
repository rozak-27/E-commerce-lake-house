FROM apache/hive:3.1.3

USER root

# Install curl
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Download JAR versi yang kompatibel dengan Hadoop 2.x
RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.10.2/hadoop-aws-2.10.2.jar \
    -o /opt/hive/lib/hadoop-aws-2.10.2.jar

RUN curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    -o /opt/hive/lib/aws-java-sdk-bundle-1.11.1026.jar

# Copy hive-site.xml dengan config MinIO
COPY hive-site.xml /opt/hive/conf/hive-site.xml

USER hive