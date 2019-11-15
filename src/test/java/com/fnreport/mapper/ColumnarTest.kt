package com.fnreport.mapper

import io.kotlintest.TestCase
import io.kotlintest.specs.StringSpec
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.flow.toList

class ColumnarTest : StringSpec() {

    val x = FixedRecordLengthFile("src/test/resources/caven20.fwf")
    val nama = arrayListOf("date", "channel", "delivered", "ret")
    val coord = arrayListOf((0 to 10), (10 to 84), (84 to 124), (124 to 164))
    val columns: List<Pair<String, Pair<Pair<Int, Int>, (Any?) -> Any?>>> = nama.zip((coord.map { it to stringMapper() }))
    @UseExperimental(InternalCoroutinesApi::class)
    val cx = Columnar(x, columns)

    override fun beforeTest(testCase: TestCase) {

    }


   init {
        "values" {
            val values = decode(1)
            System.err.println(values)
        }

        "pivot" { }

        "group" { }
    }

    @UseExperimental(InternalCoroutinesApi::class)
    private suspend fun decode(row:Int):List<Any?> {

return cx.values(1).first()


    }

}