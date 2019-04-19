this converts jdbc databases to couchdb or other REST PUT methods.

this README file is almost as long as the code.  call these scripts with no params for simple help. 

mvn install 
run from project dir

# example:

## per query:

ASYNC=true JSONINPUT=true bin/sql2json.sh episodes  podcast_id http://0.0.0.0:5984/testing_  'jdbc:mysql://0.0.0.0/foo_production?user=root&password='$pw  select \*  from oss_episode

## per tablespace:

ASYNC=true    bin/jdbc2json.sh 0.0.0.0 foo_production root $pw http://0.0.0.0:5984/cms_

## env variables:
ASYNC=true 
 * default behavior will collect a map of result code counters which will tell you how many inserts (201) succeeded, and how many fails (409) result from the REST calls
 
JSONINPUT=true 
 * this will make a best-attempt to find escaped json strings and store the unescaped version 
 
 # todo
  * [x] asyncronous REST inserts 
  * [x] reify json strings
  * [ ] bulk inserts
  * [ ] enable Gson builder configs in env
  * [ ] configurable Numerics options like squelching ".0" 
  
