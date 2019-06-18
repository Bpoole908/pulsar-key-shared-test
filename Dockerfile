FROM maven:3.6-jdk-8-slim

WORKDIR /home/demo

ADD . /home/demo

#RUN ["mvn", "clean", "package"]