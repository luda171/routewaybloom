#!/bin/bash
THE_CLASSPATH=./target/bloom-app-1.0-SNAPSHOT-jar-with-dependencies.jar
#export JAVA_HOME=/usr/lib/jvm/java-1.8.0/

java  -classpath $THE_CLASSPATH gov.lanl.bloom.web.BloomMain -port 8899 

