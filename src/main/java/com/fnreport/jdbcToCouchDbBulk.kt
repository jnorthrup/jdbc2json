package com.fnreport

import com.fasterxml.jackson.databind.ObjectMapper
import com.fnreport.jdbcToCouchDbBulk.objectMapper
import com.fnreport.jdbcToCouchDbBulk.resultSequence
import java.lang.System.*
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.text.SimpleDateFormat
import com.fnreport.PkSeqMeta.COLUMN_NAME as COLUMN_NAME1
import com.fnreport.PkSeqMeta.KEY_SEQ as KEY_SEQ1


open class EnvConfig(val name: String, val defValue: String? = null, val docString: String? = null) {
    private val env: String? by lazy { getenv(name) ?: defValue }
    val value get() = env
    override fun toString(): String = "$name${defValue?.let { ":='$defValue'" }
            ?: ""}${docString?.let { "/* $docString */" } ?: ""}"
}

object jdbcToCouchDbBulk {
    val objectMapper = ObjectMapper().apply {
        dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    }


    private val schemaa = EnvConfig("SCHEMAPATTERN")

    private val catalogg = EnvConfig("CATALOG")

    private val tablenamePattern = EnvConfig("TABLENAMEPATTERN", null, "NULL is permitted, but pattern may include '%' also")

    private val typesConfig = EnvConfig("TYPES", """["TABLE"]""",
            """array: Typical types are "TABLE",
                       "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",    \"LOCAL TEMPORARY\", \"ALIAS\", \"SYNONYM\" """)

    val configs: List<EnvConfig>
        get() = listOf(catalogg, schemaa, tablenamePattern, typesConfig)

    fun go(args: Array<out String>) {
        if (args.size < 2) {
            err.println(
                    """
                    usage:
                    env vars:
                    ${configs.joinToString(prefix = "[", postfix = "]", separator = "] [")} 
                    cmdline: 
                    ${jdbcToCouchDbBulk.javaClass.canonicalName} http://admin:somepassword@0.0.0.0:5984/prefix_ jdbc:mysql://foo 
                    """.trimIndent()
            )
            exit(1)
        }
        val driver = DriverManager.getDriver(args[1])
        val jdbcUrl = args[1]
        val connection = driver.connect(jdbcUrl, getProperties())

        err.println("driver info for '$jdbcUrl' ${driver.toString()} ")
        err.println("connection info: ${connection.clientInfo}")
        val metaData = connection.metaData
        val couchprefix = args[0]
        Triple(catalogg.value, schemaa.value, tablenamePattern.value).let { (cname, sname, tpat) ->
            val typs = typesConfig.value?.let { objectMapper.readValue(it, Array<String>::class.java) }
            val sourceTables = metaData.getTables(cname, sname, tpat, typs)
            val jdbcColumnNames = jdbcColumnNames(sourceTables.metaData)
            val jdbcRowArray = jdbcRowArray(jdbcColumnNames, sourceTables).toList()
            err.println()
        }
    }

    fun jdbcColumnNames(meta: ResultSetMetaData) = (1..meta.columnCount).map { meta.getColumnLabel(it) }
    fun jdbcRowArray(hdr: Iterable<*>, rs: ResultSet) = resultSequence(rs).map { hdr.mapIndexed { index, _ -> index + 1 }.map { rs.getObject(it) } }
    fun resultSequence(rs: ResultSet) = generateSequence { rs.takeIf { rs.next() } }
    fun jdbcColumnNameToOrdinal(dbMeta: DatabaseMetaData, cat: String?, schem: String?, tname: String, cname: String) =
            dbMeta.getColumns(cat, schem, tname, cname).also { it.next() }.getInt(ColumnMetaColumns.ORDINAL_POSITION.ordinal)


    fun jdbcTablePkNameSequence(dbMeta: DatabaseMetaData, tname: String) =
            (catalogg.value to schemaa.value).let { (cat, schem) ->
                resultSequence(dbMeta.getPrimaryKeys(cat, schem, tname)).map { it.getInt(KEY_SEQ1.ordinal) to it.getString(COLUMN_NAME1.ordinal) }
                        .sortedBy(Pair<Int, String>::first)
                        .map(Pair<Int, String>::second)
                        .map { cname -> jdbcColumnNameToOrdinal(dbMeta, cat, schem, tname, cname) }
            }
}


fun main(vararg args: String) {
    jdbcToCouchDbBulk.go(args)
}
