# Loading and Lookup service based on Bloomfilter library :
https://github.com/Baqend/Orestes-Bloomfilter
## Prerequisites:
      Java 8
      Redis db:
      see Redis Installation:
      https://redis.io/download
      Start Redis with $ redis-server. The server will listen on port 6379.

## with maven installed 
``` sh
$ mvn clean package
```
## Load predefined 16 Bloom filters to Redis db
	add list of directories where are cdx files reside to dirs.txt
        edit runcdxtobloom.sh with local parameters
./runcdxtobloom.sh
## Start Lookup service
       After loading the redis db start lookup service:

