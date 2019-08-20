package com.fnreport

import com.hazelcast.config.Config
import com.hazelcast.core.Hazelcast

fun main() {
        val configs = listOf(
            EnvConfig("HAZELCAST", (System.currentTimeMillis()).toString())
     ).map { it.name to it }.toMap()

    val hz=Hazelcast.getOrCreateHazelcastInstance(Config(configs["HAZELCAST"]!!.value))
    hz.distributedObjects.map { System.out.println(it.toString()) }
}