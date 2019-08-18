package com.fnreport

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fnreport.JdbcMacros.jdbcColumnNames
import com.fnreport.JdbcMacros.jdbcRows
import com.fnreport.JdbcMacros.jdbcTablePkOrdinalSequence
import com.fnreport.JdbcMacros.jdbcTablePkMetaPair
import java.lang.System.*
import java.sql.DriverManager
import java.sql.ResultSet
import java.text.SimpleDateFormat

object JdbcToCouchDbBulk {
    val objectMapper = ObjectMapper().apply {
        dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        this.propertyNamingStrategy = PropertyNamingStrategy.SNAKE_CASE
    }


    val fetchSize = EnvConfig("FETCHSIZE")
    val useTerse = EnvConfig("TERSE", docString = "if not blank, this will write 1 array per record after potential record '_id'  and will create a view to decorate the values as an object.")
    val schemaa = EnvConfig("SCHEMAPATTERN")
    val catalogg = EnvConfig("CATALOG")

    val tablenamePattern = EnvConfig("TABLENAMEPATTERN", null, "NULL is permitted, but pattern may include '%' also")

    val typesConfig = EnvConfig("TYPES", """["TABLE"]""",
            """array: Typical types are "TABLE",
                       "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM" """)

    val configs: List<EnvConfig>
        get() = listOf(useTerse,fetchSize, catalogg, schemaa, tablenamePattern, typesConfig)

    fun go(args: Array<out String>) {
        if (args.size < 2) {
            err.println(
                    """
                    usage:
                    env vars:
                    ${configs.joinToString(prefix = "[", postfix = "]", separator = "] [")} 
                    cmdline: 
                    ${JdbcToCouchDbBulk.javaClass.canonicalName} http://admin:somepassword@0.0.0.0:5984/prefix_ jdbc:mysql://foo 
                    """.trimIndent()
            )
            exit(1)
        }
        val driver = DriverManager.getDriver(args[1])
        val jdbcUrl = args[1]
        val connection = driver.connect(jdbcUrl, getProperties())

        err.println("driver info for '$jdbcUrl' ${driver.toString()} ")
        err.println("connection info: ${connection.clientInfo}")
        fetchSize.value?.run { err.println("setting fetchsize to: " + fetchSize.value) }
        val dbMeta = connection.metaData
        val couchprefix = args[0]
        Triple(catalogg.value, schemaa.value, tablenamePattern.value).let { (cname, sname, tpat) ->
            val typs = typesConfig.value?.let { objectMapper.readValue(it, Array<String>::class.java) }
            val sourceTables = dbMeta.getTables(cname, sname, tpat, typs)
            err.println(jdbcColumnNames(sourceTables.metaData).toString())
            val rows = jdbcRows(jdbcColumnNames(sourceTables.metaData), sourceTables)

            rows.forEach {
                val fullName = it.dropLast(2)
                var tname = fullName.last().toString()
                val pkColumns = jdbcTablePkOrdinalSequence(dbMeta, tname).toList()
                val statement = connection.createStatement()


                if (statement.execute("select count(*) from $tname")) {
                    var rowCount = statement.resultSet
                            ?.takeIf(ResultSet::next)?.getLong(1)
                            ?: 0
                    if (rowCount > 0) {
                        fetchSize.value?.run { statement.fetchSize = fetchSize.value!!.toInt() }
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

                                val row1 = jdbcRows(columnNameArray, this);
                                row1.forEach {row1->

                                    val data = columnNameArray.mapIndexed{index, s ->s to  row1[index]  }
                                    err.println(
                                            json ((when(pkColumns.size){
                                                0->emptyList<Pair <String,*>>()
                                                1->listOf ("_id" to row1[pkColumns.first()-1 ].toString())
                                                else -> listOf("_id"  to json(pkColumns.map { row1[it-1 ] }) )
                                            }+data).toMap())
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    infix fun <T> json(x: T) = objectMapper.writeValueAsString(x)


}

fun main(vararg args: String) {
    JdbcToCouchDbBulk.go(args)
}
