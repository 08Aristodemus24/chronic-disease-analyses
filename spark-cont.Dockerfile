# we need to use 3.5.5 so it is compatible with
FROM bitnami/spark:3.5.5

USER root

RUN apt update && \
    apt-get install -y curl

# add necessary jar packages for us to run our spark scripts
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.768/aws-java-sdk-core-1.12.768.jar --output /opt/bitnami/spark/jars/aws-java-sdk-core-1.12.768.jar && \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.768/aws-java-sdk-s3-1.12.768.jar --output /opt/bitnami/spark/jars/aws-java-sdk-s3-1.12.768.jar && \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-kms/1.12.768/aws-java-sdk-kms-1.12.768.jar --output /opt/bitnami/spark/jars/aws-java-sdk-kms-1.12.768.jar && \
    curl https://repo1.maven.org/maven2/com/amazonaws/jmespath-java/1.12.768/jmespath-java-1.12.768.jar --output /opt/bitnami/spark/jars/jmespath-java-1.12.768.jar && \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.563/aws-java-sdk-bundle-1.11.563.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.563.jar && \

    curl https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar --output /opt/bitnami/spark/jars/guava-27.0-jre.jar && \
    curl https://repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.16/httpcore-4.4.16.jar --output /opt/bitnami/spark/jars/httpcore-4.4.16.jar && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar --output /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar

# remove original installation of guava 14 which is outdated if
# used with aws s3
RUN rm -f /opt/bitnami/spark/jars/guava-14.0.1.jar && \
    rm -f /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar  

