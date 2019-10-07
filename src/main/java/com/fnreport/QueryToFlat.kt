package com.fnreport

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.runBlocking
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.lang.System.exit
import java.sql.DriverManager
import java.sql.JDBCType
import java.sql.ResultSet
import java.text.MessageFormat
import java.text.SimpleDateFormat
import java.util.Arrays.asList
import kotlin.math.max

/**
 * write records using deterministic _rev on the intended json
 * User: jim
 */
class QueryToFlat {
    companion object {
        val nothing = emptyList<String>()
        var x = nothing

        fun go(vararg args: String) {
            if (args.size < 1) {
                System.err.println(MessageFormat.format("dump query to stdout or \$OUTPUT \n [QUALIFY=true] [MAXLEN=Integer] [TABLENAME='tablename'] [OUTPUT='outfilename.txt'] {0} ''jdbc-url'' <sql>   ", QueryToFlat::class.java.canonicalName))
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
            val maxlen: Int? = System.getenv("MAXLEN")?.toInt()
            val qualify: Boolean = System.getenv("Qualify")?.toBoolean() ?: false
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


            lateinit var cwidths: Array<Int>
            var cnames: Array<String>
            var cmax = -1
            val cr = "\n".toByteArray()

            try {
                generateSequence { rs.takeIf { rs.next() } }.forEachIndexed { rownum, cursorRow ->
                    if (rownum == 0) {
                        cwidths = (1..cursorRow.metaData.columnCount).map {
                            cursorRow.metaData.getColumnDisplaySize(it).let {
                                maxlen?.let { max(maxlen, it) } ?: it
                            }
                        }.toTypedArray()
                        cnames = (1..cursorRow.metaData.columnCount).map {
                            if (qualify)
                                arrayOf(cursorRow.metaData.getSchemaName(it),
                                        cursorRow.metaData.getTableName(it),
                                        cursorRow.metaData.getColumnName(it)
                                ).joinToString("@")
                            else
                                cursorRow.metaData.getColumnName(it)
                        }.toTypedArray()

                        var accum = 0
                        cmax = cwidths.max()!!
                        val ofile = System.getenv("OUTPUT") ?: "fn"
                        System.err.println("""
                            # some sample pandas code
                            import pandas as pd;
                            d1names=${cnames.map { "'$it'" }}
                            d1=pd.read_fwf('$ofile', 
                            names=d1names, 
                            colspecs=${cwidths.map { i: Int -> accum to accum + i.also { accum += i } }})
                            for i in d1names: print( (i,len( d1[i].unique() )) )
                            
                            d1.to_csv('${ofile.replace(Regex("\\.fwf$"), "") + ".csv"}')
                            """.trimIndent())
                    }

                    (1..cursorRow.metaData.columnCount).forEachIndexed { i, ci ->
                        val currentColWidth = cwidths[i]
                        val oval = cursorRow.getString(ci) ?: ""

                        val outbuf = oval.toByteArray()
                        val csz = outbuf.size
                        os.write(when {
                            csz < currentColWidth -> outbuf + ByteArray(currentColWidth - csz) { ' '.toByte() }
                            csz > currentColWidth -> outbuf.copyOfRange(0, currentColWidth - 1)
                            else -> outbuf
                        })
                    }

                    os.write(cr)
                }
            } catch (t: Throwable) {
                t.printStackTrace()
            }
        }

        @JvmStatic
        fun main(vararg args: String) = runBlocking { go(*args) }
    }
}