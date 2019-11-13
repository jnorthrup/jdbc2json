package com.fnreport.mapper

import io.kotlintest.TestCase
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlinx.coroutines.flow.*
import java.nio.ByteBuffer

class FixedRecordLengthBufferTest0 : StringSpec() {

    override fun beforeTest(testCase: TestCase) {
    }

    init {
        "get" {
            val x = FixedRecordLengthBuffer("src/test/resources/caven20.fwf")
            val flow = x[0, 19, 10]

            suspend1(flow)

        }
    }

     private inline suspend fun suspend1(flow: Flow<ByteBuffer>) {
         val ccc = flow.toList()

         val reified = ccc
                 .map { byteBuffer ->
                     val bb = ByteArray(byteBuffer.remaining());
                     byteBuffer.get(bb)
                       String(bb)
//                     System.err.println(string)
                 }

         val trim = reified[1].trim()
         trim.shouldBe("2017-07-060100865/0101010106/13-14/01                                               4.000000000000                          0E-12" )
         System.err.println( trim )//.shouldBe("2017-07-060100865/0101010106/13-14/01                                               4.000000000000                          0E-12" )
         /*collect { byteBuffer ->
     val bb=ByteArray(byteBuffer.remaining());
     byteBuffer.get(bb)
     val string = String(bb)
               System.err.println (string)
 } */
//         Unit
     }
}
