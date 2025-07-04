FROM astrocrpublic.azurecr.io/runtime:3.0-4

USER root

# Atualiza repositórios e instala distutils + OpenJDK17 + ant
RUN apt-get update && \
    apt-get install -y \
      python3-distutils \
      openjdk-17-jdk \
      ant && \
    rm -rf /var/lib/apt/lists/*

# Define JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER astro
