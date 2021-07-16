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
``` sh	
./runcdxtobloom.sh
```
## Start Lookup service
       After loading the redis db start lookup service:
``` sh	       
       
       runlookupservice.sh
```       
## API   
There are two different endpoints:
http://localhost:8899/haw/{url} 
Takes a URL as parameter
If URL contains special characters, they need to be encoded
http://localhost:8899/haw/norm/{normurl}
Takes a URL as parameter
Accepts already normalized URLs, as contained in CDX files

Both endpoints return either a HTTP 200 or a HTTP 404 response code, depending on whether the URI is found in the BF or not. 


