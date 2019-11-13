package com.fnreport.mapper/*
package com.fnreport.mapper

import org.junit.Before

import org.junit.Assert.*

class MappedFwfTest {

    @Before
    fun setUp() {
        val d1names = arrayOf("date", "channel", "deliver_qty", "return_qty").map { { it } }
        val x = arrayOf((0 to 10), (10 to 84), (84 to 124), (124 to 164))
        val filemappers = mapOf (0 to DateMapper, 2 to DoubleMapper, 3 to DoubleMapper)

    }
}*/
