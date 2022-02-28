this converts jdbc databases to couchdb or other REST PUT methods.

this README file is almost as long as the code.  call these scripts with no params for simple help. 


# build
mvn install 

run from project dir using scripts in bin:

# examples:

## per query:

 
# todo
  * [x] asyncronous REST inserts 
  * [x] reify json strings
  * [X] bulk inserts
  * [X] remove gson and use Jackson
  * [ ] review kotlin serializers
  * [X] *sort of* configurable Numerics options like squelching ".0" 
  
 
# docs

each of these simple query utilities have some basic self-docs by running without parms, shown below.

each is wrapped in a shell script that figures out the how to add `lib/*.jar` to the classpath

each utility uses configuration variables from the environment and also reiterates these as config variables to stderr as `java -D` switches for next time.

the docs below are constructed by 
```bash
for i in feathersql.sh flatsql.sh  jdbc2json.sh  jdbctocouchdbbulk.sh  sql2json.sh syncsql.sh;  do echo '###' $i ;echo ;echo '```';bin/$i 2>&1 |while read;do  echo $REPLY  ;done; echo '```';echo;done
 
```

### feathersql.sh

dump small resultsets to apache arrow-feather.

```
++ dirname bin/feathersql.sh
+ JDIR=bin/../
+ exec java -classpath 'bin/..//target/*:bin/..//target/lib/*' com.fnreport.QueryToFeather
dump query to stdout or $OUTPUT
[TABLE=tablename] [OUTPUT=outfilename.txt] com.fnreport.QueryToFeather 'jdbc-url' <sql>
```

### flatsql.sh

dump resultsets to pandas fwf as stdout - stderr has pandas/python preamble

```
bin/flatsql.sh

dump query to stdout or $OUTPUT
[TABLENAME=tablename] [OUTPUT=outfilename.txt] bin/flatsql.sh 'jdbc-url' <sql>

```

### jdbc2json.sh

(deprecated) simple couchdb writer from all tables.

```
++ dirname bin/jdbc2json.sh
+ JDIR=bin/../
+ java -Drest.async=false -classpath 'bin/.././target/*:bin/.././target/lib/*' com.fnreport.ToJson
copy all tables to json PUT
[ASYNC=true] [JSONINPUT=true] com.fnreport.ToJson dbhost dbname user password couchprefix [jdbc:url:etc]
```

### jdbctocouchdbbulk.sh

writes connection catalog  query parameters as couchdb bulk inserts  

```
++ dirname bin/jdbctocouchdbbulk.sh
+ JDIR=bin/../
+ exec java -classpath 'bin/..//target/*:bin/..//target/lib/*' com.fnreport.JdbcToCouchDbBulkKt
usage:
env vars:
[FETCHSIZE/* number of rows to fetch from jdbc */] [BULKSIZE:='500'/* number of rows to write in bulk */] [BATCHMODE/* ifnotnull */] [TERSE:='false'/* if not blank, this will write 1 array per record after potential record '_id' and will create a view to decorate the values as an object. */] [SCHEMAPATTERN] [CATALOG] [TABLENAMEPATTERN/* NULL is permitted, but pattern may include '%' also */] [TYPES:='["TABLE"]'/* array: Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM" */]
cmdline:
com.fnreport.JdbcToCouchDbBulk http://[admin:somepassword]@0.0.0.0:5984/prefix_ jdbc:mysql://foo base64_basic_authentication_token
```

#### Example: Writing connection to Vertica and Pulling Types ["Views"] with bin/jdbctocouchdbbulk.sh (*TO BE UPDATED PROPERLY*)

```shell

TYPES=[\"VIEW\"] TERSE=true /opt/como_initializer/jdbc2json/bin/jdbctocouchdbbulk.sh http://$HOST:5984/como_dw_ jdbc:vertica:VerticaHost:portNumber/databaseName?user=username&password=password $COUCHDB_BASIC_AUTH

```

### sql2json.sh

writes a sql query to couchdb

```
++ dirname bin/sql2json.sh
+ JDIR=bin/../
+ exec java -classpath 'bin/..//target/*:bin/..//target/lib/*' com.fnreport.SqlExecToJson
convert a query to json (and PUT to url)
[ASYNC=true] [JSONINPUT=true] com.fnreport.SqlExecToJson name pkname couch_prefix 'jdbc-url' <sql>
```

### syncsql.sh

reads a couchdb table and a sql query and runs bulk add/update/delete of the delta

```
++ dirname bin/syncsql.sh
+ JDIR=bin/../
+ exec java -classpath 'bin/..//target/*:bin/..//target/lib/*' com.fnreport.ReiterateDb
convert a query to json (and PUT to url)
[SORTINTS=false] [ALLORNOTHING=true] [JSONINPUT=false] com.fnreport.ReiterateDb name pkname couch_prefix 'jdbc-url' <sql>
```

 
