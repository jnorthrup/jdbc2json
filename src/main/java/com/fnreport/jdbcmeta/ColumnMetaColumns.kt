package com.fnreport.jdbcmeta

/**  * <P>Only column descriptions matching the catalog, schema, table
 * and column name criteria are returned.  They are ordered by
 * <code>TABLE_CAT</code>,<code>TABLE_SCHEM</code>,
 * <code>TABLE_NAME</code>, and <code>ORDINAL_POSITION</code>.
 *
 * <P>Each column description has the following columns:
 *  <OL>
 *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
 *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
 *  <LI><B>TABLE_NAME</B> String {@code =>} table name
 *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
 *  <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
 *  <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name,
 *  for a UDT the type name is fully qualified
 *  <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
 *  <LI><B>BUFFER_LENGTH</B> is not used.
 *  <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
 * DECIMAL_DIGITS is not applicable.
 *  <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
 *  <LI><B>NULLABLE</B> int {@code =>} is NULL allowed.
 *      <UL>
 *      <LI> columnNoNulls - might not allow <code>NULL</code> values
 *      <LI> columnNullable - definitely allows <code>NULL</code> values
 *      <LI> columnNullableUnknown - nullability unknown
 *      </UL>
 *  <LI><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
 *  <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be <code>null</code>)
 *  <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
 *  <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
 *  <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
 *       maximum number of bytes in the column
 *  <LI><B>ORDINAL_POSITION</B> int {@code =>} index of column in table
 *      (starting at 1)
 *  <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
 *       <UL>
 *       <LI> YES           --- if the column can include NULLs
 *       <LI> NO            --- if the column cannot include NULLs
 *       <LI> empty string  --- if the nullability for the
 * column is unknown
 *       </UL>
 *  <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope
 *      of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
 *  <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope
 *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
 *  <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope
 *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
 *  <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
 *      Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE
 *      isn't DISTINCT or user-generated REF)
 *   <LI><B>IS_AUTOINCREMENT</B> String  {@code =>} Indicates whether this column is auto incremented
 *       <UL>
 *       <LI> YES           --- if the column is auto incremented
 *       <LI> NO            --- if the column is not auto incremented
 *       <LI> empty string  --- if it cannot be determined whether the column is auto incremented
 *       </UL>
 *   <LI><B>IS_GENERATEDCOLUMN</B> String  {@code =>} Indicates whether this is a generated column
 *       <UL>
 *       <LI> YES           --- if this a generated column
 *       <LI> NO            --- if this not a generated column
 *       <LI> empty string  --- if it cannot be determined whether this is a generated column
 *       </UL>
 *  </OL>
 *
 * <p>The COLUMN_SIZE column specifies the column size for the given column.
 * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
 * For datetime datatypes, this is the length in characters of the String representation (assuming the
 * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
 * this is the length in bytes. Null is returned for data types where the
 * column size is not applicable.
 */
enum class ColumnMetaColumns : JDBCMetaOffset {
    UNUSED,
    TABLE_CAT,
    TABLE_SCHEM,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    TYPE_NAME,
    COLUMN_SIZE,
    BUFFER_LENGTH,
    DECIMAL_DIGITS,
    NUM_PREC_RADIX,
    NULLABLE,
    REMARKS,
    COLUMN_DEF,
    SQL_DATA_TYPE,
    SQL_DATETIME_SUB,
    CHAR_OCTET_LENGTH,
    ORDINAL_POSITION,
    IS_NULLABLE,
    SCOPE_CATALOG,
    SCOPE_SCHEMA,
    SCOPE_TABLE,
    SOURCE_DATA_TYPE,
    IS_AUTOINCREMENT,
    IS_GENERATEDCOLUMN;

}