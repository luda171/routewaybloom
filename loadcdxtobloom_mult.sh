#!/bin/bash
THE_CLASSPATH=/data2/bloom-app/target/bloom-app-1.0-SNAPSHOT-jar-with-dependencies.jar
export JAVA_HOME=/usr/lib/jvm/java-1.8.0/
for FILE in  $(ls /data1/haw/);
do
echo $FILE 
/usr/lib/jvm/jre-1.8.0/bin/java -Xmx4g  -classpath $THE_CLASSPATH  gov.lanl.bloom.utils.MultipleBloomLoader -dir /data1/haw/  -filename $FILE
echo
done;
