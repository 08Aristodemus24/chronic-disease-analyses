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
RUN 
    # check for updates and install them
    apt update && \
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

# switch to airflow user right after setting env variables
USER airflow

# copy and install dependencies in airflow container specifically
# in the /opt/airflow directory which is teh airflow home
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt