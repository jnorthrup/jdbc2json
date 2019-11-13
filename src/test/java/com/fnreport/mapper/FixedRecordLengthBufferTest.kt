package com.fnreport.mapper

import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.flowOf
import org.junit.Test

import org.junit.Assert.*
import org.junit.Before

class FixedRecordLengthBufferTest {

    private lateinit var origin: FixedRecordLengthBuffer

    @Before
fun setup(){
    this.origin=FixedRecordLengthBuffer("src/test/resources/caven20.fwf")
}

    @Test
    fun get() {
        val flow1 = origin[1]
          
        System.err.println(flow1)
        System.err.println(         origin[19])
     }
}