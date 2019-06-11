package com.fnreport

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.InputStreamReader
import java.lang.System.exit
import java.lang.System.out
import java.net.HttpURLConnection
import java.net.URL
import java.sql.DriverManager
import java.sql.SQLException
import java.text.MessageFormat
import java.text.SimpleDateFormat
import java.util.*
import java.util.Arrays.asList

/**
 * write records using deterministic _rev on the intended json
 * User: jim
 */
object ReiterateDb {


    val USEJSONINPUT = System.getenv("JSONINPUT") == "true"
    val ASYNC = System.getenv("ASYNC") == "true"

    internal var counter: Long = 0


    suspend fun go(vararg args: String) {
        if (args.size < 1) {
            System.err.println(MessageFormat.format("convert a query to json (and PUT to url) \n [ASYNC=true] [JSONINPUT=true] {0} name pkname couch_prefix ''jdbc-url''  <sql>   ", ReiterateDb::class.java.canonicalName))
            exit(1)
        }
        val couchDbName = args[0]
        val pkname = args[1]
        val couchPrefix = args[2]
        val jdbcUrl = args[3]

        val objectMapper = ObjectMapper()
        objectMapper.dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        System.err.println("\"use json rows\" is $USEJSONINPUT")
        System.err.println("\"rest.async\" is $ASYNC")

//        var rows= mutableMapOf <String,Pair<String,Map<String,Any?>>>();

        var rows: Map<String, Pair<String, SortedMap<String, Any?>>> = mapOf()

        lateinit var deleted: List<Map<String, Any>>
        lateinit var added: List<Map<String, Any>>
        lateinit var delta: List<Map<String, Any>>
        lateinit var jdbcRows: Map<String, Map<String, Any>>

        runBlocking {
            val cjob = launch {
                try {
                    val urlConnection: HttpURLConnection = URL(couchPrefix + '/' + couchDbName + '/' + "_all_docs?include_docs=true").openConnection() as HttpURLConnection
                    val couchRes = objectMapper.readValue(InputStreamReader(urlConnection.inputStream), Map::class.java)

                    rows = ((couchRes["rows"] as List<Map<String, Any?>>)).filter { it["id"].toString().startsWith("_").not() }
                            .map { rowMap -> rowMap["doc"] as Map<String, Any?> }.map { docMap: Map<String, Any?> -> docMap["_id"].toString() to (docMap["_rev"].toString() to docMap.filter { (key, vl) -> !key.startsWith("_") && vl != null }.toSortedMap()) }.toMap()
                } catch (_: Throwable) {
                }
            }


            val jjob = launch {
                val sql = asList(*args).subList(4, args.size).joinToString(" ")
                System.err.println("using sql: $sql")
                try {
                    val DRIVER = DriverManager.getDriver(jdbcUrl)

                    val rs = DRIVER.connect(jdbcUrl, null).createStatement().executeQuery(sql)
                    val metaData = rs.metaData
                    jdbcRows = generateSequence { rs.takeIf { it.next() } }.map {
                        (1..metaData.columnCount).map { cno ->
                            metaData.getColumnName(cno) to
                                    it.getObject(cno).let { s ->

                                        s.takeIf { it is String && it.isNotBlank() && it.length > 1 }?.toString()?.trim()?.let { it ->
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
                    }.map { map -> map[pkname] as String to objectMapper.readValue(objectMapper.writeValueAsString(map), Map::class.java) }.toMap() as Map<String, Map<String, Any>>
                } catch (e: SQLException) {
                    e.printStackTrace()
                    exit(1)

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

            }

            (cjob).join()
            (jjob).join()
        }

        val  out=object {
            val docs = deleted + added + delta
        }

//        System.err.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(deleted + added + delta))
        objectMapper.writerWithDefaultPrettyPrinter().writeValue( System.out,out)

    }
}

suspend fun main(vararg args: String) {
    ReiterateDb.go(*args)
}