package com.fnreport

import org.intellij.lang.annotations.Language
import java.io.IOException
import java.lang.System.err
import java.lang.System.exit
import java.net.HttpURLConnection
import java.net.URL
import java.sql.ResultSet
import kotlin.system.exitProcess
import kotlin.text.Charsets.UTF_8

object JdbcToCouchDbBulk {


    val configs = listOf(
        EnvConfig("FETCHSIZE", docString = "number of rows to fetch from jdbc"),
        EnvConfig("BULKSIZE", "500", docString = "number of rows to write in bulk"),
        EnvConfig("BATCHMODE", docString = "ifnotnull"),
        EnvConfig(
            "TERSE",
            "false",
            docString = "if not blank, this will write 1 array per record after potential record '_id'  and will create a view to decorate the values as an object."
        ),
        EnvConfig("SCHEMAPATTERN"),
        EnvConfig("LIMIT", null, "fetch only this many rows per entity if set"),
        EnvConfig("CATALOG"),
        EnvConfig(
            "QUOTES",
            "[]",
            "the dialect of sql you are using may require quotes specific to jdbc driver like single or double ticks or index brackets."
        ),
        EnvConfig("TABLENAMEPATTERN", null, "NULL is permitted, but pattern may include '%' also"),
        EnvConfig(
            "TYPES",
            """["TABLE"]""",
            """JSON array: Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM" """
        )
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
        ) = arrayOf("FETCHSIZE", "CATALOG", "SCHEMAPATTERN", "TABLENAMEPATTERN", "TYPES").map { configs.get(it)!! }
            .map(EnvConfig::value)
        fetchSize?.run { err.println("setting fetchsize to: " + fetchSize) }

        val terse = configs["TERSE"]!!.value?.toBoolean() ?: false


        val bulkSize = configs["BULKSIZE"]!!.value!!.toInt()
        val limit = configs["LIMIT"]?.value?.toInt()
        val couchprefix = args[0]
        val couchauth = args[2]
        val authheader = "Basic " + couchauth
        val (qopen, qclose) = configs["QUOTES"]!!.value!!.map { it }

        listOf(catalogg, schemaa, tablenamePattern).let { (cname, sname, tpat) ->
            val typs = typesConfig?.let { objectMapper.readValue(it, Array<String>::class.java) }
            val catalogResultSet = dbMeta.getTables(cname, sname, tpat, typs)
            val hdr = catalogResultSet.metaData.jdbcColumnNames
            err.println(hdr.toString())
            val catalogRows = catalogResultSet.jdbcRows(hdr)

            catalogRows.map { dbMeta.jdbcEntity(hdr to it) }.forEach { e ->
                val statement = connection.createStatement()

                if (statement.execute("select count(*) from $qopen${e.tname}$qclose")) {
                    val rowCount = statement.resultSet?.takeIf(ResultSet::next)?.getLong(1) ?: 0
                    if (rowCount > 0) {
                        fetchSize?.run { statement.fetchSize = fetchSize.toInt() }
                        with(statement) {
                            val lstring = limit?.let { " LIMIT $limit" } ?: ""
                            execute("select * from ${qopen}${e.tname}${qclose}$lstring")
                            with(resultSet) {
                                val columnNameArray by lazy { metaData.jdbcColumnNames }
                                val columnTypeArray by lazy { (1..columnNameArray.size).map(metaData::getColumnTypeName) }
                                val columnLenArray by lazy { (1..columnNameArray.size).map(metaData::getColumnDisplaySize) }
                                val viewHeader by lazy {
                                    val id = e.pkColumnIndexes.takeUnless(List<Int>::isEmpty)?.let {
                                        try {
                                            "${json(e.pkColumnIndexes.map { i -> columnNameArray[i - 1] })}"
                                        } catch (e: IndexOutOfBoundsException) {
                                            e.printStackTrace()
                                        }
                                    } ?: "auto"
                                    "for tname: ${e.tname} rows: $rowCount, pkeys: ${e.pkColumnIndexes.size}/${e.pkColumnIndexes.toList()} columns: $columnNameArray  id: $id"

                                }
                                err.println("$viewHeader")

                                val couchTable = couchprefix + e.tname.toLowerCase()
                                var couchConn = URL(couchTable).openConnection() as HttpURLConnection
                                couchConn.requestMethod = "PUT"
                                couchConn.setRequestProperty("Content-Type", "application/json")
                                couchConn.setRequestProperty("Accept", "application/json")
                                couchConn.setRequestProperty("Authorization", authheader)
                                couchConn.doOutput = true
                                couchConn.outputStream.write("".toByteArray())

                                err.println(couchTable + ": " + couchConn.responseCode to couchConn.responseMessage)
                                couchConn.disconnect()
                                if (terse) {
                                    couchConn = URL(couchTable).openConnection() as HttpURLConnection
                                    couchConn.requestMethod = "POST"
                                    couchConn.setRequestProperty("Content-Type", "application/json")
                                    couchConn.setRequestProperty("Accept", "application/json")
                                    couchConn.setRequestProperty("Authorization", authheader)
                                    couchConn.doOutput = true
                                    @Language("JavaScript") val viewCode = """
                                        function (doc) {
                                                        var pkeys = ${json(e.pkColumnIndexes)};
                                                        var columns = ${json(columnNameArray)}; 
                                                        var e = {};  
                                                        var row = doc.row;
                                                        var length = row.length;
                                                        for (var i = 0; i < length; i++) e[columns[i]] = row[i];
                                                        emit(${if (e.pkColumnIndexes.size < 2) "doc._id" else "JSON.parse(doc._id)"}, e)
                                                    }""".trimMargin()
                                    val terseViewsString = json(
                                        mapOf(
                                            "_id" to "_design/meta",
                                            "desc" to mapOf(
                                                "names" to columnNameArray,
                                                "types" to columnTypeArray,
                                                "lengths" to columnLenArray
                                            ),
                                            "views" to mapOf(
                                                "asMap" to mapOf(
                                                    "map" to (viewCode)
                                                )
                                            ),
                                            "language" to "javascript"
                                        )
                                    )

                                    err.println("attempting to write:\n" + terseViewsString + "\n----------")
                                    couchConn.outputStream.write(terseViewsString.toByteArray(UTF_8))

                                    err.println(couchTable + ": " + couchConn.responseCode to couchConn.responseMessage)
                                    couchConn.disconnect()
                                }
                                val row = this.jdbcRows(columnNameArray)

                                row.chunked(bulkSize).forEach { rowChunk ->
                                    try {
                                        couchConn =
                                            URL(couchTable + "/_bulk_docs").openConnection() as HttpURLConnection
                                        couchConn.requestMethod = "POST"
                                        couchConn.setRequestProperty("Content-Type", "application/json")
                                        couchConn.setRequestProperty("Authorization", authheader)
                                        couchConn.doInput = true
                                        couchConn.doOutput = true
                                    } catch (e: IOException) {
                                        err.println(
                                            "" +
                                                    couchConn.responseCode + " : " + couchConn.responseMessage
                                        )
                                        e.printStackTrace()
                                        exitProcess(1)
                                    }

                                    couchConn.outputStream.write(json(mapOf("docs" to rowChunk.map { row ->
                                        if (!terse)
                                            rowAsMap(columnNameArray, e.pkColumnIndexes, row)
                                        else
                                            rowAsTuples(e.pkColumnIndexes, row).toMap()
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
