package com.fnreport

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.InputStreamReader
import java.lang.System.exit
import java.net.HttpURLConnection
import java.net.URL
import java.sql.DriverManager
import java.sql.JDBCType
import java.sql.SQLException
import java.text.MessageFormat
import java.text.SimpleDateFormat
import java.util.*
import java.util.Arrays.asList
import kotlin.system.measureTimeMillis

/**
 * write records using deterministic _rev on the intended json
 * User: jim
 */
class ReiterateDb {

    companion object {
        val ALLORNOTHING = System.getenv("ALLORNOTHING") == "true"
        val SORTINTS = System.getenv("SORTINTS") != "false"
        val USEJSONINPUT = System.getenv("JSONINPUT") != "false"

        internal var counter: Long = 0


        suspend fun go(vararg args: String) {
            if (args.size < 1) {
                System.err.println(MessageFormat.format("convert a query to json (and PUT to url) \n  [SORTINTS=false] [ALLORNOTHING=true] [JSONINPUT=false] {0} name pkname couch_prefix ''jdbc-url''  <sql>   ", ReiterateDb::class.java.canonicalName))
                exit(1)
            }
            val couchDbName = args[0]
            val couchPrefix = args[2]
            val jdbcUrl = args[3]

            val objectMapper = ObjectMapper()
            objectMapper.dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
            System.err.println("\"use json rows\" is $USEJSONINPUT")
            System.err.println("\"sort integer keys\" is $SORTINTS")

//        var rows= mutableMapOf <String,Pair<String,Map<String,Any?>>>();

            var rows: Map<String, Pair<String, SortedMap<String, Any?>>> = mapOf()

            lateinit var deleted: List<Map<String, Any>>
            lateinit var added: List<Map<String, Any>>
            lateinit var delta: List<Map<String, Any>>
            lateinit var jdbcRows: Map<String, Map<String, Any>>
            var sqlms: Long = -1
            var couchms: Long = -1
            runBlocking {
                val cjob = launch {

                    try {
                        couchms = measureTimeMillis {
                            val urlConnection: HttpURLConnection = URL(couchPrefix + '/' + couchDbName + '/' + "_all_docs?include_docs=true").openConnection() as HttpURLConnection
                            val couchRes = objectMapper.readValue(InputStreamReader(urlConnection.inputStream), Map::class.java)
                            rows = (couchRes["rows"] as List<Map<String, *>>).filter { it["id"].toString().startsWith("_").not() }
                                    .map { rowMap -> rowMap["doc"] as Map<String, *> }.map { docMap: Map<String, Any?> -> docMap["_id"].toString() to (docMap["_rev"].toString() to docMap.filter { (key, vl) -> !key.startsWith("_") && vl != null }.toSortedMap()) }.toMap()
                        }
                    } catch (e: Throwable) {
                        System.err.println(e.localizedMessage)
                    }
                }


                val jjob = launch {
                    val sql = asList(*args).subList(4, args.size).joinToString(" ")
                    val pkname = args[1]

                    System.err.println("using sql: $sql")
                    try {
                        val DRIVER = DriverManager.getDriver(jdbcUrl)
                        var pktype: JDBCType = JDBCType.DECIMAL
                        sqlms = measureTimeMillis {
                            val rs = DRIVER.connect(jdbcUrl, null).createStatement().executeQuery(sql)
                            val metaData = rs.metaData
                            var pkCol = 0
                            (1..metaData.columnCount).forEach {
                                val columnLabel = metaData.getColumnLabel(it)
                                val jdbcType = JDBCType.valueOf(metaData.getColumnType(it))
                                if (columnLabel == pkname) {
                                    pkCol = it
                                    pktype = jdbcType
                                }
                                System.err.println("$columnLabel using ${if (pkCol == it) "pk_" else "sql_"}type $pktype")
                            }


                            var pkfun: (Any) -> String = fun (any:Any):String{   return any.toString() }

                            if (SORTINTS) {
                                pkfun = when (pktype) {
                                    in arrayOf(
                                            JDBCType.BIGINT,
                                            JDBCType.NUMERIC
                                    ) -> fun (any:Any):String{ return (("$any".toLong()) + 100_0000_0000_0000_0000L).toString().drop(1) }
                                    in arrayOf(
                                            JDBCType.INTEGER
                                    ) -> fun (any:Any):String{ return  (("$any".toLong()) + 5_000_000_000L).toString().drop(1) }
                                    in arrayOf(
                                            JDBCType.TINYINT,
                                            JDBCType.SMALLINT
                                    ) -> fun (any:Any):String { return (("$any".toLong()) + 4_000_000L).toString().drop(1) }
                                    else -> pkfun;
                                }
                            }
                            jdbcRows = generateSequence { rs.takeIf { it.next() } }.map {
                                (1..metaData.columnCount).map { cno ->
                                    metaData.getColumnName(cno) to it.getObject(cno).let { s ->
                                        s.takeIf { USEJSONINPUT && it is String && it.isNotBlank() && it.length > 1 }?.toString()?.trim()?.let { it ->
                                            when (it.first() to it.last()) {
                                                '[' to ']' ->
                                                    objectMapper.readValue(it, List::class.java)
                                                '{' to '}' ->
                                                    objectMapper.readValue(it, Map::class.java)
                                                else -> it
                                            }
                                        } ?: s
                                    }
                                }.filter { (f, s) -> s != null }.sortedBy { (f, s) -> f }.toMap()
                            }.map { map ->
                                pkfun(map[pkname]!!) to
                                        objectMapper.readValue(objectMapper.writeValueAsString(map), Map::class.java)
                            }.toMap() as Map<String, Map<String, Any>>
                        }
                    } catch (e: SQLException) {
                        e.printStackTrace()
                        exit(1)

                    }


                }

                (cjob).join()
                (jjob).join()

            }
            val intersect = jdbcRows.keys.intersect(rows.keys)
            deleted = (rows.keys - intersect).map { k -> mapOf("_id" to k, "_deleted" to true, "_rev" to rows[k]!!.first) }
            added = (jdbcRows.keys - intersect).map { jdbcRows[it]!! + ("_id" to it) }

            delta = intersect.mapNotNull { k ->
                val newMap = jdbcRows[k]!!
                k.takeIf {
                    rows[k]!!.second.toString() != newMap.toString()
                }?.let { newMap + ("_id" to k) + ("_rev" to rows[k]!!.first) }
            }
            val out = object {
                val docs = deleted + added + delta
                val all_or_nothing = ALLORNOTHING

                val _meta = object {
                    val timings = object {
                        val couchDbTime = couchms
                        val sqlTime = sqlms
                    }
                    val keys = object {
                        val deleted = deleted.map { it["_id"] }
                        val added = added.map { it["_id"] }
                        val delta = delta.map { it["_id"] }
                    }
                    val counts = object {
                        val rows = docs.size
                        val deleted = deleted.size
                        val added = added.size
                        val delta = delta.size
                    }

                }
            }
//        System.err.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(deleted + added + delta))
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(System.out, out)

        }
        @JvmStatic
        fun main(vararg args: String) = runBlocking { ReiterateDb.go(*args) }
    }
}