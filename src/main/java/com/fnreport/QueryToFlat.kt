package com.fnreport

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.lang.System.exit
import java.sql.*
import java.text.MessageFormat
import java.text.SimpleDateFormat
import java.util.*
import java.util.Arrays.asList
import kotlin.system.measureTimeMillis

/**
 * write records using deterministic _rev on the intended json
 * User: jim
 */
class QueryToFlat {

    companion object {

        internal var counter: Long = 0


        suspend fun go(vararg args: String) {
            if (args.size < 1) {
                System.err.println(MessageFormat.format("dump query to stdout or \$OUTPUT \n [TABLE='tablename'] [OUTPUT='outfilename.txt'] {0} ''jdbc-url'' <sql>   ", QueryToFlat::class.java.canonicalName))
                exit(1)
            }
            val jdbcUrl = args[0]

            val objectMapper = ObjectMapper()
            objectMapper.dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

//        var rows= mutableMapOf <String,Pair<String,Map<String,Any?>>>();

            var rows: Map<String, Pair<String, SortedMap<String, Any?>>> = mapOf()

            lateinit var deleted: List<Map<String, Any>>
            lateinit var added: List<Map<String, Any>>
            lateinit var delta: List<Map<String, Any>>
            var sqlms: Long = -1
            var couchms: Long = -1
            runBlocking {
                val jjob = launch {
                    val sql = asList(*args).subList(1, args.size).joinToString(" ")

                    System.err.println("using sql: $sql")
                    val meta = mutableMapOf<String, Map<String, Map<String, Any?>>>()
                    try {
                        val DRIVER = DriverManager.getDriver(jdbcUrl)
                        sqlms = measureTimeMillis {

                            val conn = DRIVER.connect(jdbcUrl, null)

                            val mastermeta = System.getenv("TABLENAME")?.let {
                                it.split(",")
                            }?.forEach { tname ->
                                System.err.println("loading meta for $tname")

                                conn.metaData.getColumns(null, null, tname, null).also { rs: ResultSet ->
                                    val x = mutableListOf<String>()
                                    (1..rs.metaData.columnCount).forEach { cnum: Int -> x += rs.metaData.getColumnName(cnum).toUpperCase() }
                                    meta[tname] = generateSequence {
                                        takeIf { rs.next() }?.let {
                                            if (rs.row == 1) System.err.println("potential meta" to x)
                                            rs.getString("COLUMN_NAME") to
                                                    x.map { mname ->
                                                        mname.toUpperCase() to
                                                                rs.getObject(mname)
                                                    }.toMap<String, Any?>()
                                        }
                                    }.toMap()
                                }
                            }
                            System.err.println(meta.map { (k, v) ->
                                k to
                                        v.map { (k, m) ->
                                            m["COLUMN_NAME"] to
                                                    (JDBCType.values()[
                                                            m["SQL_DATA_TYPE"] as Int] to
                                                            m["CHAR_OCTET_LENGTH"])
                                        }
                            })

                            val rs = conn.createStatement().executeQuery(sql)
                            val metaData = rs.metaData
                            var pkCol = 0

                            data class flatSchemaColMeta(val name: String, val startcol: Int, val fieldLen: Int, val sqlType: SQLType)

                            var lastlen = 0


                            (1..metaData.columnCount).map { cnum ->
                                val typ: JDBCType = JDBCType.valueOf(metaData.getColumnType(cnum))
                                val columnType1 = metaData.getColumnType(cnum)
                                val columnType = columnType1
                                columnType

//                                flatSchemaColMeta(metaData.getColumnLabel(it),


                            }


                            var pkfun: (Any) -> String = fun(any: Any): String { return any.toString() }

                            generateSequence { rs.takeIf { it.next() } }.map {
                                (1..metaData.columnCount).map { cno ->
                                    metaData.getColumnName(cno) to it.getObject(cno).let { s ->
                                        s
                                    }
                                }
                            }
                        }
                    } catch (e: SQLException) {
                        e.printStackTrace()
                        exit(1)

                    }


                }

//                (cjob).join()
                (jjob).join()

            }
//        System.err.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(deleted + added + delta))
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(System.out, {})

        }

        @JvmStatic
        fun main(vararg args: String) = runBlocking { go(*args) }
    }
}