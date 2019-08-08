package com.fnreport

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.lang.System.exit
import java.sql.*
import java.text.DateFormat
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
        val nothing = emptyList<String>()
        var x = nothing

        suspend fun go(vararg args: String) {
            if (args.size < 1) {
                System.err.println(MessageFormat.format("dump query to stdout or \$OUTPUT \n [TABLE='tablename'] [OUTPUT='outfilename.txt'] {0} ''jdbc-url'' <sql>   ", QueryToFlat::class.java.canonicalName))
                exit(1)
            }
            val jdbcUrl = args[0]

            val objectMapper = ObjectMapper()
            objectMapper.dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

            runBlocking {
                val jjob = launch {
                    val sql = asList(*args).subList(1, args.size).joinToString(" ")

                    System.err.println("using sql: $sql")
                    val meta = mutableMapOf<String, Map<String, Map<String, Any?>>>()
                    try {
                        val DRIVER = DriverManager.getDriver(jdbcUrl)
                        measureTimeMillis {

                            val conn = DRIVER.connect(jdbcUrl, System.getProperties())
                            System.getenv("TABLENAME")?.split(",")?.forEach { tname ->
                                System.err.println("loading meta for $tname")

                                conn.metaData.getColumns(null, null, tname, null).also { rs ->
                                    val m by lazy {
                                        x.takeIf { it != nothing }
                                                ?: (1..rs.metaData.columnCount).map(rs.metaData::getColumnName).also {
                                                    x = it;
                                                    System.err.println("potential meta" to it)
                                                }
                                    }
                                    meta[tname] = generateSequence {
                                        takeIf { rs.next() }?.let {
                                            rs.getString("column_name") to m.map { mname -> mname to rs.getObject(mname) }.toMap<String, Any?>()
                                        }
                                    }.toMap()
                                }
                            }
                            System.err.println(meta.map { (k, v) -> k to v.map { (k, m) -> m["column_name"] to (JDBCType.values()[m["sql_data_type"] as Int] to m["char_octet_length"]) } })

                            val rs = conn.createStatement().executeQuery(sql)
                            val metaData = rs.metaData
                            var pkCol = 0

                            data class flatSchemaColMeta(val name: String, val startcol: Int, val fieldLen: Int, val sqlType: SQLType)

                            var lastlen = 0;


                            (1..metaData.columnCount).map { cnum ->
                                val typ: JDBCType = JDBCType.valueOf(metaData.getColumnType(cnum))
                                val columnType1 = metaData.getColumnType(cnum)
                                val columnType = columnType1
                                columnType
                            }




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