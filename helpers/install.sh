#!/bin/sh
mvn clean package;
mvn install:install-file -Dfile=./target/basilisk-vertx-client.jar -DgroupId=com.milestone.basilisk \
  -DartifactId=vertx-client -Dversion=0.1.0 -Dpackaging=jar;
