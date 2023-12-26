# RedisBulkOperation
 
Bulk operation in redis based on key pattern.

### Supported operations:
Count - Count the number of keys.
CountNoTtl - Count keys with no ttl.
CountTtl - Count the keys with a ttl larger than the one given in the ttl parameter.
Delete - Delete keys.
DeleteNoTtl - Delete keys with no expiration.
Ttl - set expiration given in the ttl parameter to keys with no expiration.

### Java:

Create a properties file with the following content (or modify the rbo.properties):

operation=<operation to perform, default=Count> 
host=<redis host, default=127.0.0.1>
port=<redis port number, default=6379>
user=<redis user, default="" (default user)>
password=<password, default="">
ssl=<true|false default=false>
sni=<sni default="">
pattern=<key pattern to operate on, default=* (all keys)>
bulk=<how many keys to scan at each iteration, default=10000>
ttl=<ttl for setting or testing>

Either compile the project, or use the compiled jar file and run as follows:
`java -cp ./RedisBulkOperations.jar com.redis.RedisBulkOperations <properties file path>`


### Python
```
usage: RedisBulkOperations.py [-h] [-s, --host HOST] [-p, --port PORT] [-u, --user USER] [-w, password PASSWORD]
                              [-o, --operation OPERATION] [-k, --key-pattern PATTERN] [-t, --ttl TTL] [-b, --bulk BULK]
                              [-l, --ssl SSL] [--key-file KEY] [--cert-file CERT] [--ca-file CA]
optional arguments:
  -h, --help            show this help message and exit
  -s, --host HOST       Server address (default: localhost)
  -p, --port PORT       Server port (default: 6379)
  -u, --user USER       User name (default: )
  -w, --password PASSWORD
                        Password (default: )
  -o, --operation OPERATION
                        Operation: (count|countNoTtl|countTtl|delete|deleteNoTtl|prinNoTtl|ttl) (default: count)
  -k, --key-pattern PATTERN
                        Key pattern (default: *)
  -t, --ttl TTL         TTL in seconds (default: -1)
  -b, --bulk BULK       Bulk size (default: 1000)
  -l, --ssl SSL         SSL connection (default: False)
  --key-file KEY        Private key file (default: None)
  --cert-file CERT      Certificate file (default: None)
  --ca-file CA          CA file (default: None)
```
  
