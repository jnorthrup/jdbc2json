package com.fnreport

import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

/**
 * maps a single pandas "fwf" fixed-width-file to mmap, limitted by ByteBuffer inherrent size limit  to 2.5 gigs.
 *
 * this uses NIO buffers and cleanup is sometimes imperfect, the distance between close()
 * and process destruction should be short.
 */
class MappedDataFrame(
        val fn: String,
        val fieldMeta: List<Pair<String, Pair<Int, Int>>>,
        typeMap: Map<Int, FieldParser<*>> = emptyMap(),
        val mappers: Array<FieldParser<*>> = Array(fieldMeta.size) { typeMap[it] ?: Utf8String },
        val randomAccessFile: RandomAccessFile = RandomAccessFile(fn, "r"),
        val channel: FileChannel = randomAccessFile.channel!!,
        val mappedByteBuffer: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, randomAccessFile.length()),
        val recordLen: Int = run {
            val lastField = fieldMeta.last()
            lastField.let { (_, pair) ->
                pair.let { (start, end) ->
                    val handle = mappedByteBuffer.duplicate().position(end)
                    var c: Byte
                    do c = handle.get() while (c != '\n'.toByte())
                    handle.position()
                }
            }
        },
        val size: Long = randomAccessFile.length() / recordLen

) : Closeable by randomAccessFile {

    fun recordAsMap(index: Int, singleton: Boolean = false) =
            when (singleton) {
                true -> mappedByteBuffer
                false -> mappedByteBuffer.duplicate()
            }.position(recordLen * index).let { byteBuffer ->
                byteBuffer.slice().let { buf ->
                    fieldMeta.zip(mappers).map { (field, mapper) ->
                        field.let { (name, range) ->
                            name to sliceValue(mapper, range, buf)
                        }
                    }
                }
            }

    fun recordAsList(index: Int, singleton: Boolean = false) =
            when (singleton) {
                true -> mappedByteBuffer
                false -> mappedByteBuffer.duplicate()
            }.position(recordLen * index).let { byteBuffer ->
                byteBuffer.slice().let { buf ->
                    fieldMeta.zip(mappers).map { (field, mapper) ->

                        field.let { (_, range) ->
                            sliceValue(mapper, range, buf)
                        }
                    }
                }
            }


    private fun sliceValue(
            mapper: FieldParser<*>,
            range: Pair<Int, Int>,
            buf: ByteBuffer
    ): Any? {
        return mapper(range.let { (start, end) ->
            ByteArray(end - start).also {
                buf.position(start)[it]
            }
        })
    }
}