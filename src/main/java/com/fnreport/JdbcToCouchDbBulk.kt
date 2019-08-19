package com.fnreport

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fnreport.JdbcMacros.connectToJdbcUrl
import com.fnreport.JdbcMacros.jdbcColumnNames
import com.fnreport.JdbcMacros.jdbcRows
import com.fnreport.JdbcMacros.jdbcTablePkOrdinalSequence
import com.fnreport.JdbcMacros.json
import com.fnreport.JdbcMacros.rowAsMap
import com.fnreport.JdbcMacros.rowAsTuples
import org.intellij.lang.annotations.Language
import java.io.IOException
import java.lang.System.*
import java.net.HttpURLConnection
import java.net.URL
import java.sql.ResultSet
import java.text.SimpleDateFormat
import kotlin.system.exitProcess
import kotlin.text.Charsets.UTF_8

object JdbcToCouchDbBulk {
    public val objectMapper = ObjectMapper().apply {
        dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        this.propertyNamingStrategy = PropertyNamingStrategy.SNAKE_CASE
    }


    val configs = listOf(
            EnvConfig("FETCHSIZE", docString = "number of rows to fetch from jdbc"),
            EnvConfig("BULKSIZE", "500", docString = "number of rows to write in bulk"),
            EnvConfig("BATCHMODE", docString = "ifnotnull"),
            EnvConfig("TERSE", "false", docString = "if not blank, this will write 1 array per record after potential record '_id'  and will create a view to decorate the values as an object."),
            EnvConfig("SCHEMAPATTERN"),
            EnvConfig("CATALOG"),
            EnvConfig("TABLENAMEPATTERN", null, "NULL is permitted, but pattern may include '%' also"),
            EnvConfig("TYPES", """["TABLE"]""", """array: Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM" """)
    ).map { it.name to it }.toMap()


    fun go(args: Array<out String>) {
        if ("--help" in args || args.size < 2) {
            printOptions()
        }

        val (connection, dbMeta) = connectToJdbcUrl(jdbcUrl = args[1])

        val (fetchSize,
                catalogg,
                schemaa,
                tablenamePattern,
                typesConfig
        ) = arrayOf("FETCHSIZE", "CATALOG", "SCHEMAPATTERN", "TABLENAMEPATTERN", "TYPES").map { configs.get(it)!! }.map(EnvConfig::value)
        fetchSize?.run { err.println("setting fetchsize to: " + fetchSize) }

        val terse = configs["TERSE"]!!.value?.toBoolean() ?: false


        val bulkSize = configs["BULKSIZE"]!!.value!!.toInt()
        val couchprefix = args[0]


        listOf(catalogg, schemaa, tablenamePattern).let { (cname, sname, tpat) ->
            val typs = typesConfig?.let { objectMapper.readValue(it, Array<String>::class.java) }
            val catalogResultSet = dbMeta.getTables(cname, sname, tpat, typs)
            err.println(jdbcColumnNames(catalogResultSet.metaData).toString())
            val catalogRows = jdbcRows(jdbcColumnNames(catalogResultSet.metaData), catalogResultSet)

            catalogRows.forEach {
                val fullName = it.dropLast(2)
                val tname = fullName.last().toString()
                val pkColumns = jdbcTablePkOrdinalSequence(dbMeta, tname).toList()
                val statement = connection.createStatement()

                if (statement.execute("select count(*) from $tname")) {
                    val rowCount = statement.resultSet?.takeIf(ResultSet::next)?.getLong(1) ?: 0
                    if (rowCount > 0) {
                        fetchSize?.run { statement.fetchSize = fetchSize.toInt() }
                        with(statement) {
                            execute("select * from $tname")
                            with(resultSet) {
                                val columnNameArray by lazy { metaData.let(::jdbcColumnNames) }
                                val viewHeader by lazy {
                                    val id = pkColumns.takeUnless(List<Int>::isEmpty)?.let {
                                        try {
                                            "${json(pkColumns.map { columnNameArray[it - 1] })}"
                                        } catch (e: IndexOutOfBoundsException) {
                                            e.printStackTrace()
                                        }
                                    } ?: "auto"
                                    "for tname: $tname rows: $rowCount, pkeys: ${pkColumns.size}/${pkColumns.toList()} columns: $columnNameArray  id: $id"

                                }
                                err.println("$viewHeader")

                                val couchTable = couchprefix + tname.toLowerCase()
                                var couchConn = URL(couchTable).openConnection() as HttpURLConnection
                                couchConn.requestMethod = "PUT"
                                couchConn.setRequestProperty("Content-Type", "application/json")
                                couchConn.doOutput = true
                                couchConn.outputStream.write("".toByteArray())

                                err.println(couchTable + ": " + couchConn.responseCode to couchConn.responseMessage)
                                couchConn.disconnect()
                                if (terse) {
                                    couchConn = URL(couchTable).openConnection() as HttpURLConnection
                                    couchConn.requestMethod = "POST"
                                    couchConn.setRequestProperty("Content-Type", "application/json")
                                    couchConn.doOutput = true
                                    @Language("JavaScript") val viewCode = """
                                        function (doc) {
                                                        var pkeys = ${json(pkColumns)};
                                                        var columns = ${json(columnNameArray)};
                                                        var e = {}; 
                                                    
                                                        var row = doc.row;
                                                        var length = row.length;
                                                        for (var i = 0; i < length; i++) e[columns[i]] = row[i];
                                                        emit(${if (pkColumns.size < 2) "doc._id" else "JSON.parse(doc._id)"}, e)
                                                    }""".trimIndent()
                                    val terseViewsString = json(mapOf("_id" to "_design/meta",
                                            "views" to mapOf(
                                                    "asMap" to mapOf(
                                                            "map" to  (viewCode)
                                                    )
                                            ),
                                            "language" to "javascript"))

                                    err.println("attempting to write:\n" + terseViewsString + "\n----------")
                                    couchConn.outputStream.write(terseViewsString.toByteArray(UTF_8))

                                    err.println(couchTable + ": " + couchConn.responseCode to couchConn.responseMessage)
                                    couchConn.disconnect()
                                }
                                val row = jdbcRows(columnNameArray, this)

                                row.chunked(bulkSize).forEach { rowChunk ->
                                    try {
                                        couchConn = URL(couchTable + "/_bulk_docs").openConnection() as HttpURLConnection
                                        couchConn.requestMethod = "POST"
                                        couchConn.setRequestProperty("Content-Type", "application/json")
                                        couchConn.doInput = true
                                        couchConn.doOutput = true
                                    } catch (e: IOException) {
                                        err.println("" +
                                                couchConn.responseCode + " : " + couchConn.responseMessage
                                        )
                                        e.printStackTrace()
                                        exitProcess(1)
                                    }

                                    couchConn.outputStream.write(json(mapOf("docs" to rowChunk.map { row ->
                                        if (!terse)
                                            rowAsMap(columnNameArray, pkColumns, row)
                                        else
                                            rowAsTuples(pkColumns, row).toMap()
                                    })).toByteArray(UTF_8))
                                    couchConn.outputStream.close()
                                    err.println("${couchConn.url} : ${couchConn.responseCode} : ${couchConn.responseMessage}")
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private fun printOptions() {
        err.println(
                """
                        usage:
                        env vars:
                        ${configs.values.joinToString(prefix = "[", postfix = "]", separator = "] [")} 
                        cmdline: 
                        ${JdbcToCouchDbBulk.javaClass.canonicalName} http://[admin:somepassword]@0.0.0.0:5984/prefix_ jdbc:mysql://foo 
                        """.trimIndent()
        )
        exit(1)
    }
}

fun main(vararg args: String) {
    JdbcToCouchDbBulk.go(args)
}
