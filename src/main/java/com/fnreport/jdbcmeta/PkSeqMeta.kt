package com.fnreport.jdbcmeta
/**     * <P>Each primary key column description has the following columns:
 *  <OL>
 *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
 *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
 *  <LI><B>TABLE_NAME</B> String {@code =>} table name
 *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
 *  <LI><B>KEY_SEQ</B> short {@code =>} sequence number within primary key( a value
 *  of 1 represents the first column of the primary key, a value of 2 would
 *  represent the second column within the primary key).
 *  <LI><B>PK_NAME</B> String {@code =>} primary key name (may be <code>null</code>)
 *  </OL>
 */
enum class PkSeqMeta {
    UNUSED,
    TABLE_CAT,// String => table catalog(may be null)
    TABLE_SCHEM,// String => table schema(may be null)
    TABLE_NAME, //String => table name
    COLUMN_NAME, //String => column name
    KEY_SEQ;// short => sequence number within primary key (a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key).

}