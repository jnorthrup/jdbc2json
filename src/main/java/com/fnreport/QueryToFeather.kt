package com.fnreport

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.runBlocking
import org.apache.arrow.adapter.jdbc.JdbcToArrow
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.ArrowFileWriter
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.lang.System.exit
import java.nio.channels.Channels
import java.sql.DriverManager
import java.sql.JDBCType
import java.sql.ResultSet
import java.text.MessageFormat
import java.text.SimpleDateFormat
import java.util.Arrays.asList

/**
 * write records using deterministic _rev on the intended json
 * User: jim
 */
class QueryToFeather {
    companion object {
        val nothing = emptyList<String>()
        var x = nothing

        fun go(vararg args: String) {
            if (args.size < 1) {
                System.err.println(MessageFormat.format("dump query to stdout or \$OUTPUT \n [TABLE='tablename'] [OUTPUT='outfilename.txt'] {0} ''jdbc-url'' <sql>   ", QueryToFeather::class.java.canonicalName))
                exit(1)
            }
            val jdbcUrl = args[0]

            val objectMapper = ObjectMapper()

            objectMapper.dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

            val sql = asList(*args).subList(1, args.size).joinToString(" ")

            System.err.println("using sql: $sql")
            val meta = mutableMapOf<String, Map<String, Map<String, Any?>>>()
            val DRIVER = DriverManager.getDriver(jdbcUrl)
            val conn = DRIVER.connect(jdbcUrl, System.getProperties())
            System.getenv("TABLENAME")?.split(",")?.forEach { tname ->
                System.err.println("loading meta for $tname")

                conn.metaData.getColumns(null, null, tname, null).also { rs: ResultSet ->
                    val x = (1..rs.metaData.columnCount).map(rs.metaData::getColumnName).map(String::toUpperCase)
                    meta[tname] = generateSequence {
                        takeIf { rs.next() }?.let {
                            if (rs.row == 1) System.err.println("driver-specific potential meta" to x)
                            rs.getString("COLUMN_NAME") to x.map { it to rs.getObject(it) }.toMap()
                        }
                    }.toMap()
                }
            }

            System.err.println(meta.map { (k, v) ->
                k to v.map { (_, m) ->
                    val any = m["SQL_DATA_TYPE"].takeUnless { it?.toString()?.toInt() == 0 }
                            ?: m["DATA_TYPE"].toString().toInt()
                    val jdbcType = JDBCType.values()[any as Int]
                    m["COLUMN_NAME"] to (jdbcType.toString() + "($any)" to m["COLUMN_SIZE"])
                }
            })

            System.err.println(objectMapper.writeValueAsString(meta))
            val rs = conn.createStatement().executeQuery(sql)
            val os = System.getenv("OUTPUT")?.let { BufferedOutputStream(FileOutputStream(it)) } ?: System.out
            val sqlToArrow = JdbcToArrow.sqlToArrow(rs)
            val arrowFileWriter = ArrowFileWriter(sqlToArrow, DictionaryProvider.MapDictionaryProvider(), Channels.newChannel(os))
            arrowFileWriter.writeBatch()

            arrowFileWriter.end()
            arrowFileWriter.close()
            sqlToArrow.close()
        }

        @JvmStatic
        fun main(vararg args: String) = runBlocking {
            System.setProperty("io.netty.tryReflectionSetAccessible", "true") //https://issues.apache.org/jira/browse/ARROW-5412?focusedCommentId=16861192&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-16861192
            go(*args)
        }
    }
}