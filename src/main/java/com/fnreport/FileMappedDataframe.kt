package com.fnreport

import java.io.Closeable
import java.io.RandomAccessFile
import java.lang.Integer.max
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.math.min
import kotlin.system.measureTimeMillis


/**
 * maps a single pandas "fwf" fixed-width-file to mmap, limitted by ByteBuffer inherrent size limit  to 2.x gigs.
 *
 * the file format (I have encountered) is an EOL terminated record with otherwise bounded chars and spaces between.
 *
 * This format gives an absolute random access ISAM contract ideal for sparse access via mmap and compresses within
 * a few percent of csv.
 *
 * this uses NIO buffers and cleanup is sometimes imperfect, the distance between close()
 * and process destruction should be short.
 *
48M   caven.csv
11M   caven.csv.zst
54M   caven.feather
13M   caven.feather.zst
144M   caven.fwf
9.4M   caven.fwf.zst      <-- not bad...
144M   cavez.fwf          <-- sorted
5.4M   cavez.fwf.zst      wow!
 *
 */
class FileMappedDataframe(
    fn: String?,
    fieldMeta: List<ParseDescriptor>,
    typeMap: Map<Int, FieldParser<*>> = emptyMap(),
    mappers: Array<FieldParser<*>> = Array(fieldMeta.size) { typeMap[it] ?: StringMapper },
    randomAccessFile: RandomAccessFile = RandomAccessFile(fn!!, "r"),
    channel: FileChannel = randomAccessFile.channel!!,
    override val buffer: MappedByteBuffer = channel.map(
        FileChannel.MapMode.READ_ONLY,
        0,
        randomAccessFile.length()
    ),
    recordLen: Int = run {
        fieldMeta.last().let { (_, pair) ->
            pair.let { (_, end) ->
                buffer.mark().position(end).apply {
                    var c: Byte
                    do c = this.get() while (c != '\n'.toByte())
                }.position().also { buffer.reset() }
            }
        }
    }

) : Closeable by randomAccessFile, ByteDataFrame(
    fieldMeta.map { (a, b) -> a to b }.zip(mappers).toTypedArray(),
    buffer,
    recordLen,
    (randomAccessFile.length() / recordLen).toInt()
)

