this converts mysql databases to couchdb or other REST PUT methods.

this is a java/maven uberjar design which creates an executable jarfile


ubuntu instructions:

sudo apt-get install maven2 couchdb

cd ./jmysqltojson

mvn assembly:assembly

#if all builds well, this is the next step

cd target

java -jar  jmysql2json-0.0.1-SNAPSHOT-jar-with-dependencies.jar mysqlhost mysqlinstance  mysqluser mysqlpassword http://localhost:5984/couchdb

# (5 paramters, no slash at end of url)
1. mysqlhost 
2. mysqlinstance 
3. mysqluser 
4. mysqlpassword 
5. http://localhost:5984/couchdb