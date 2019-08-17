package com.fnreport.jdbcmeta
/**TABLE_CAT String => table catalog (may be null)
TABLE_SCHEM String => table schema (may be null)
TABLE_NAME String => table name
TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
REMARKS String => explanatory comment on the table (may be null)
TYPE_CAT String => the types catalog (may be null)
TYPE_SCHEM String => the types schema (may be null)
TYPE_NAME String => type name (may be null)
SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
*/
enum class TableMeta {
    UNUSED,
    TABLE_CAT,// String => table catalog (may be null)
    TABLE_SCHEM,// String => table schema (may be null)
    TABLE_NAME,// String => table name
    TABLE_TYPE,// String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
    REMARKS,// String => explanatory comment on the table (may be null)
    TYPE_CAT,// String => the types catalog (may be null)
    TYPE_SCHEM,// String => the types schema (may be null)
    TYPE_NAME,// String => type name (may be null)
    SELF_REFERENCING_COL_NAME,// String => name of the designated "identifier" column of a typed table (may be null)
    REF_GENERATION,// String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
}