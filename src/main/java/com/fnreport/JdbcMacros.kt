package com.fnreport


import com.fnreport.JdbcToCouchDbBulk.configs
import com.fnreport.jdbcmeta.ColumnMetaColumns
import com.fnreport.jdbcmeta.PkSeqMeta
import java.lang.System.err
import java.lang.System.getProperties
import java.sql.*
import java.util.*

object JdbcMacros {
    infix fun <T> json(x: T) = JdbcToCouchDbBulk.objectMapper.writeValueAsString(x)
    fun jdbcColumnNames(meta: ResultSetMetaData) = (1..meta.columnCount).map { meta.getColumnLabel(it) }
    fun jdbcRows(hdr: Iterable<*>, rs: ResultSet) = resultSequence(rs).map { hdr.mapIndexed { index, _ -> index + 1 }.map { rs.getObject(it) } }
    fun resultSequence(rs: ResultSet) = generateSequence { rs.takeIf { rs.next() } }
    fun jdbcColumnNameToOrdinal(dbMeta: DatabaseMetaData, cat: String?, schem: String?, tname: String, cname: String) =
            dbMeta.getColumns(cat, schem, tname, cname).also { it.next() }.getInt(ColumnMetaColumns.ORDINAL_POSITION.ordinal)


    /**
     * provided with a databasemetadata, we get the primaryKeyColumnIndexes
     * (1 extra database call after getting label/name)
     *
     * starts from 1
     */
    fun jdbcTablePkOrdinalSequence(dbMeta: DatabaseMetaData,
                                   tname: String,
                                   schemaName: String? = configs["SCHEMA"]?.value,
                                   catalogName: String? = configs["CATALOG"]?.value
    ) = jdbcTablePkColNameSequence(dbMeta, tname, schemaName, catalogName).map { cname -> jdbcColumnNameToOrdinal(dbMeta, catalogName, schemaName, tname, cname) }

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

    fun jdbcTablePkMetaPair(dbMeta: DatabaseMetaData, tname: String,
                            schemaName: String? = configs["SCHEMA"]?.value,
                            catalogName: String? = configs["CATALOG"]?.value
    ) = resultSequence(dbMeta.getPrimaryKeys(catalogName, schemaName, tname)).map { it.getInt(PkSeqMeta.KEY_SEQ.ordinal) to it.getString(PkSeqMeta.COLUMN_NAME.ordinal) }

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
}