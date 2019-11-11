# JN Toolkit

This is a collection of brutally transparent and efficient solutions to Data Engineering and Machine Learning prep.

This self-contained and batteries included set of middleware tools enables all forms of transfer between relational,
 nosql, and machine learning leveraging the fundamentals that haven't changed since the early days of comamndline 
 scripting.


### MappedFileTable -- a completely immutable pandas clone built against numerous tensorflow POC's 

while its nice that a mature product like pandas can provide so many versatile solutions, efficiency is not one 
of its virtues. 

millesecond operations for the following big data import usecases:

    written to close the gap on existing relational engines and pandas/numpy options which have a sharp efficiency 
    drop with these frequent usecases on large datasets

  - [x]  Immutable Memory mapped FWF flat files 
 
   -  [x]  Instantaneous zero-copy data manipulations
 
       * Sparse GroupBy - instant 
       
       * Column transformation  filtering - instant
       
       * Resample Time Series Data
       
       
       
 -  [x]  Idiomatic predicate, Filtering, Column Remapping 
 
 - [x]  Sql->FWF short path scripts in the appendix 
    

### Couchdb midpoint conversions - 
  while your IT department spends months or years ramping up on migration strategies, these scripts help you move
   terabytes of relational data for analytics without bothering the IT guys with tooling and logins.  includes 
   cron-activated relationnal couch delta bulk update for pure coolness factor.
   
 
### Sysadmin Friendly 
 
 * dead simple maven build, and maven wrapper for bare linux installs. 

 * pragmatic lib/*.jar innovation handles the toughest java deployment headaches.


`./mvnw install`
  
 
# Conversion Scripts
see also 
```bash
for i in feathersql.sh flatsql.sh  jdbc2json.sh  jdbctocouchdbbulk.sh  sql2json.sh syncsql.sh;  do echo '###' $i ;echo ;echo '```';bin/$i 2>&1 |while read;do  echo $REPLY  ;done; echo '```';echo;done
 
```

### feathersql.sh

dump small resultsets to arrow.

```
++ dirname bin/feathersql.sh
+ JDIR=bin/../
+ exec java -classpath 'bin/..//target/*:bin/..//target/lib/*' com.fnreport.QueryToFeather
dump query to stdout or $OUTPUT
[TABLE=tablename] [OUTPUT=outfilename.txt] bin/feathersql.sh 'jdbc-url' <sql>
```

### flatsql.sh

dump resultsets to pandas fwf - stderr has preamble

```
++ dirname bin/flatsql.sh
+ JDIR=bin/../
+ exec java -classpath 'bin/..//target/*:bin/..//target/lib/*' com.fnreport.QueryToFlat
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
[ASYNC=true] [JSONINPUT=true] bin/jdbc2json.sh dbhost dbname user password couchprefix [jdbc:url:etc]
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
bin/jdbctocouchdbbulk.sh http://[admin:somepassword]@0.0.0.0:5984/prefix_ jdbc:mysql://foo
```

### sql2json.sh

writes a sql query to couchdb

```
++ dirname bin/sql2json.sh
+ JDIR=bin/../
+ exec java -classpath 'bin/..//target/*:bin/..//target/lib/*' com.fnreport.SqlExecToJson
convert a query to json (and PUT to url)
[ASYNC=true] [JSONINPUT=true] bin/sql2json.sh name pkname couch_prefix 'jdbc-url' <sql>
```

### syncsql.sh

reads a couchdb table and a sql query and runs bulk add/update/delete of the delta

```
++ dirname bin/syncsql.sh
+ JDIR=bin/../
+ exec java -classpath 'bin/..//target/*:bin/..//target/lib/*' com.fnreport.ReiterateDb
convert a query to json (and PUT to url)
[SORTINTS=false] [ALLORNOTHING=true] [JSONINPUT=false] bin/syncsql.sh name pkname couch_prefix 'jdbc-url' <sql>
```

 