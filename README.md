# Loading and Lookup service based on Bloomfilter library :
https://github.com/Baqend/Orestes-Bloomfilter
## Prerequisites:
      Java 8
      Redis db:
      see Redis Installation:
      https://redis.io/download
      For simple check start Redis with $ redis-server. The server will listen on port 6379.
      I used https://redis.io/topics/quickstart "Installing Redis more properly" section
      config  /etc/redis/6379.conf 
      sudo /etc/init.d/redis_6379 start
      some redis-cli commands:
      redis-cli save
      redis-cli shutdown
      redis-cli KEYS*
                INFO keyspace
 
## with maven installed 
``` sh
$ mvn clean package
```
## Load predefined 16 Bloom filters to Redis db
	
        edit loadcdxtobloom.sh or loadcdxtobloom_mult.sh  with local parameters
``` sh
	
./loadcdxtobloom_mult.sh
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
## LANL C number
LANL C22036 
