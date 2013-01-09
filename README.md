Twitter Streaming Daemon
========================

A Command line tool for pulling a filtered Twitter stream and placing the tweets into a Redis set.

Run it via 
          java -jar TwitterStreamDaemon-0.0.1-SNAPSHOT-jar-with-dependencies.jar
          
Don't forget your command line options....

          usage: -a <consumerkey>,<consumersecret>,<accesstoken>,<accesstokensecret> -r
          <redisurl> -k <keyspace> [-f <userid1>,... | -t <term1>,<term2>]
          -a,--auth <arg>       OAuth Credentials.
          -f,--follow <arg>     User IDs to follow
          -k,--keyspace <arg>   Redis keyspace to put tweets in
          -r,--redis <arg>      Redis connection URI
          -t,--track            Terms to track.

TODO:
=====
* Add comments and tests
* Ensure compatibility with "Rivers"
* Ensure long-lived Jedis client is reliable
