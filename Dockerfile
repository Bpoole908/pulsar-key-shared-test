#FROM maven:3.6-jdk-8-slim as workspace
FROM jupyter-env:3.6.7
WORKDIR /home/ben/demo

USER root 
# Install jdk and maven
RUN apt-get update && apt-get install \
    -yq --no-install-recommends \
    openjdk-8-jdk \
    maven 

RUN rm -rf /var/cache/openjdk-jdk8-installer \
    && rm -rf /var/cache/maven-installer \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*  \
    && rm -rf build

USER ben
# Install pip packages
# Do NOT add a '/' on end of .egg file! 
# ISSUE:3.7.* does not seem to work with easy_install (searchs online repo).
COPY ./build ./build
RUN python -m easy_install build/pulsar_client-2.4.0-py3.6-linux-x86_64.egg 
RUN pip install \
    -r build/requirements.txt \
    --no-cache-dir 