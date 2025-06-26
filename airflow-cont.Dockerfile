FROM apache/airflow:3.0.2-python3.11

USER root

# Installs the ff:
# * OpenJDK-17
# * Apache ant - is a Java-based build automation tool. It's 
# similar in purpose to tools like Make (for C/C++ projects), 
# Maven, or Gradle (other popular Java build tools).
# * wget
# * unzip
# * chrome binary for linux docker container
RUN apt update && \
    apt-get install -y openjdk-17-jdk wget unzip && \
    apt-get install -y ant && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean;

# Set JAVA_HOME
# if in macos use 
# ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64/"

# add necessary jar packages for us to run our spark scripts
# RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.563/aws-java-sdk-bundle-1.11.563.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.563.jar && \
#     curl https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar --output /opt/bitnami/jars/guava-27.0-jre.jar && \
#     curl https://repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.16/httpcore-4.4.16.jar --output /opt/bitnami/jars/httpcore-4.4.16.jar && \
#     curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar --output /opt/bitnami/jars/hadoop-aws-3.3.4.jar

# switch to airflow user right after setting env variables
USER airflow

# copy and install dependencies in airflow container specifically
# in the /opt/airflow directory which is teh airflow home
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt