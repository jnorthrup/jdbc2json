package com.fnreport

import com.hazelcast.config.Config
import com.hazelcast.core.Hazelcast

object JdbcToHazelCast {


    val configs = listOf(
            EnvConfig("FETCHSIZE", docString = "number of rows to fetch from jdbc"),
            EnvConfig("BULKSIZE", "500", docString = "number of rows to write in bulk"),
            EnvConfig("BATCHMODE", docString = "ifnotnull"),
            EnvConfig("TERSE", "false", docString = "if not blank, this will write 1 array per record after potential record '_id'  and will create a view to decorate the values as an object."),
            EnvConfig("SCHEMAPATTERN"),
            EnvConfig("CATALOG"),
            EnvConfig("HAZELCAST", (System.currentTimeMillis()).toString()),
            EnvConfig("TABLENAMEPATTERN", null, "NULL is permitted, but pattern may include '%' also"),
            EnvConfig("TYPES", """["TABLE"]""", """array: Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM" """)
    ).map { it.name to it }.toMap()


    fun go(args: Array<out String>) {
        if ("--help" in args || args.size < 1) {
            printOptions()
        }

        val hzName = configs["HAZELCAST"]!!.value

        val jdbcUrl = args[1]
        val (connection, dbMeta) = connectToJdbcUrl(jdbcUrl)

        val (fetchSize,
                catName,
                schemaName,
                tablenamePattern,
                typesConfig) = arrayOf("FETCHSIZE", "CATALOG", "SCHEMAPATTERN", "TABLENAMEPATTERN", "TYPES").map { configs.get(it)!! }.map(EnvConfig::value)
        fetchSize?.run { System.err.println("setting fetchsize to: " + fetchSize) }

        val typs = typesConfig?.let { objectMapper.readValue(it, Array<String>::class.java) }
        val catalogResultSet = dbMeta.getTables(catName, schemaName, tablenamePattern, typs)

        val catalogRows = catalogResultSet.jdbcRows(catalogResultSet.metaData.jdbcColumnNames)

        val entities = catalogRows.map(dbMeta::jdbcEntity)

        val hz = Hazelcast.getOrCreateHazelcastInstance(Config(hzName))

        val destination = hz.getMap<String, List<String>>((  jdbcUrl.replace(Regex("(user|username|pass|password)=[^;]+[;]?"),"$1=...;") ).toString())
        val toMap = entities.map { (hier, tname, pk) ->
            tname to hier
        }.toMap(                 destination          )




    }


    private fun printOptions() {
        System.err.println(
                """ 
                      usage:
                        env vars:
                        ${configs.values.joinToString(prefix = "[", postfix = "]", separator = "] [")} 
                        cmdline: 
                        ${JdbcToHazelCast.javaClass.canonicalName}   jdbc:mysql://foo 
                        """.trimIndent()
        )
        System.exit(1)
    }
}


fun main(vararg args: String) {
    JdbcToHazelCast.go(args)
}
