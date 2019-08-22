package com.fnreport


import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fnreport.JdbcToCouchDbBulk.configs
import com.fnreport.jdbcmeta.ColumnMetaColumns
import com.fnreport.jdbcmeta.PkSeqMeta
import java.lang.System.err
import java.lang.System.getProperties
import java.sql.*
import java.text.SimpleDateFormat
import java.util.*


val objectMapper = ObjectMapper().apply {
    dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    this.propertyNamingStrategy = PropertyNamingStrategy.SNAKE_CASE
}

fun DatabaseMetaData.jdbcEntity(catalogEntry: List<Any>): DbEntity {
    val catalogEntry1 = catalogEntry
    val dropLast = catalogEntry1!!.slice(0..3)
    val map = dropLast.map{ it?.toString()}
    return map!!.let{ hierarchy ->
        val last = hierarchy.last()
        last.let { tname ->
            val columns = getColumns(hierarchy[0], hierarchy[1], tname, null)
            val hdr = ColumnMetaColumns.values()
            columns.jdbcRows(hdr)
            DbEntity(hierarchy, tname, jdbcTablePkOrdinals(hierarchy[0], hierarchy[1], tname).toList())
        }
    }
}


/**
 * provided with a databasemetadata, we get the primaryKeyColumnIndexes
 * (1 extra database call after getting label/name)
 *
 * starts from 1
 */
fun DatabaseMetaData.jdbcTablePkOrdinals(catalogName: String?,
                                         schemaName: String?,
                                         tname: String
) =
        jdbcTablePkColNameSequence(this, tname, schemaName, catalogName).map { cname -> this.jdbcColumnNameToOrdinal(catalogName, schemaName, tname, cname) }

val ResultSetMetaData.jdbcColumnNames get() = (1..columnCount).map { getColumnLabel(it) }
fun <T> json(x: T) = objectMapper.writeValueAsString(x)
fun ResultSet.jdbcRows(hdr: List<*>) = this.resultSequence().map { hdr.mapIndexed { index, _ -> index + 1 }.map(this::getObject) }
fun ResultSet.jdbcRows(hdr: Array<*>) = this.jdbcRows(arrayListOf(hdr))
fun ResultSet.resultSequence() = generateSequence { takeIf { next() } }
fun DatabaseMetaData.jdbcColumnNameToOrdinal(cat: String?, schem: String?, tname: String, cname: String) =
        getColumns(cat, schem, tname, cname).also { it.next() }.getInt(ColumnMetaColumns.ORDINAL_POSITION.ordinal)

fun Connection.tableScan(tname: String): Pair<ResultSetMetaData, Sequence<List<Any>>> {
    val statement = createStatement()
    statement.execute("select * from $tname")
    val resultSet = statement.resultSet
    val rsMetadata = resultSet.metaData



    return rsMetadata to resultSet.jdbcRows(rsMetadata.jdbcColumnNames)
}

/**
 * provided with a databasemetadata, we get the primaryKeyColumnIndexes
 * Retrieves a description of the given table's primary key columns.  They
 * are re-ordered by KEY_SEQ .
 *
 *## Each primary key column description has the following columns:
 *
 *  * <B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
 *  * <B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
 *  * <B>TABLE_NAME</B> String {@code =>} table name
 *  * <B>COLUMN_NAME</B> String {@code =>} column name
 *  * <B>KEY_SEQ</B> short {@code =>} sequence number within primary key( a value
 *  of 1 represents the first column of the primary key, a value of 2 would
 *  represent the second column within the primary key).
 *  * <B>PK_NAME</B> String {@code =>} primary key name (may be <code>null</code>)
 *  </OL>
 *  @return the table's column names
 */
fun jdbcTablePkColNameSequence(dbMeta: DatabaseMetaData, tname: String,
                               schemaName: String? = configs["SCHEMA"]?.value,
                               catalogName: String? = configs["CATALOG"]?.value
) = jdbcTablePkMetaPair(dbMeta, tname, schemaName, catalogName)
        .sortedBy(Pair<Int, String>::first)
        .map(Pair<Int, String>::second)

private fun jdbcTablePkMetaPair(dbMeta: DatabaseMetaData, tname: String,
                                schemaName: String? = configs["SCHEMA"]?.value,
                                catalogName: String? = configs["CATALOG"]?.value
) = dbMeta.getPrimaryKeys(catalogName, schemaName, tname).resultSequence().map { it.getInt(PkSeqMeta.KEY_SEQ.ordinal) to it.getString(PkSeqMeta.COLUMN_NAME.ordinal) }

fun rowAsTuples(pkColumns: List<Int>, row: List<Any>, pkName: String = "_id") = row.let { data ->
    when (pkColumns.size) {
        0 -> emptyList<Pair<String, *>>()
        1 -> listOf(pkName to row[pkColumns.first() - 1].toString())
        else -> listOf(pkName to json(pkColumns.map { row[it - 1] }))
    } + listOf("row" to data)
}

fun rowAsMap(columnNameArray: List<String>, pkColumns: List<Int>, row: List<*>, pkName: String = "_id") =
        columnNameArray.mapIndexed { index, s -> s to row[index] }.let { data ->
            when (pkColumns.size) {
                0 -> emptyList<Pair<String, *>>()
                1 -> listOf(pkName to row[pkColumns.first() - 1].toString())
                else -> listOf(pkName to json(pkColumns.map { row[it - 1] }))
            } + data
        }.toMap()


fun connectToJdbcUrl(jdbcUrl: String, properties: Properties? = getProperties()): Pair<Connection, DatabaseMetaData> {
    val driver = DriverManager.getDriver(jdbcUrl)
    val connection = driver.connect(jdbcUrl, properties)
    err.println("driver info for '$jdbcUrl' $driver ")
    err.println("connection info: ${connection.clientInfo}")
    val dbMeta = connection.metaData
    return Pair(connection, dbMeta)
}
