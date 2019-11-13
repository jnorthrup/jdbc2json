package com.fnreport.mapper

 import kotlinx.coroutines.InternalCoroutinesApi
 import kotlinx.coroutines.flow.Flow
 import kotlinx.coroutines.flow.FlowCollector
 import kotlinx.coroutines.flow.flowOf
 import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

typealias Column = Pair<String, (Any?) -> Any?>
typealias Coordinates = Pair<Int, Int>
typealias Decoder = Triple<Column, Coordinates, (ByteArray,Coordinates) -> Any?>



interface RowStore<T> {

    /**
     * seek to row
     */

    operator fun invoke(row: Int): T
}


interface FixedRowStore<T> : Scalar<Lazy<ByteBuffer>> {
    val recordLen: Int
    val size: Int
}

interface ColumnAccess<T> {
    operator fun get(vararg c: Int): RowStore<T>
    val columns: List<Column>
}

abstract class FileAccess(open val filename: String) : Closeable


interface IDataFrame<T> : RowStore<T>, ColumnAccess<T>


//todo: map multiple segments for a very big file
open class MappedFile(
        filename: String,
        randomAccessFile: RandomAccessFile = RandomAccessFile(filename, "r"),
        channel: FileChannel = randomAccessFile.channel,
        length: Long = randomAccessFile.length(),
        val mappedByteBuffer: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, length)
) : RowStore<ByteBuffer>, FileAccess(
        filename), Closeable by randomAccessFile {
    override fun invoke(row: Int): ByteBuffer = mappedByteBuffer.apply { position(row) }.slice()
}


//todo: map multiple segments for a very big file

abstract class LineBuffer :Scalar<Flow<ByteBuffer>>

class FixedRecordLengthBuffer(filename: String,private val origin: MappedFile = MappedFile(filename),
                              val recordLen: Int = origin.mappedByteBuffer.run {
                                  var c = 0.toByte();
                                  do c =  get() while (c != '\n'.toByte())
                                  position()
                              },
                              val size: Int = (recordLen / origin.mappedByteBuffer.limit()).toInt()
)        : LineBuffer(),Closeable by origin{
    override fun   get(row: Int) = flowOf(   origin(recordLen*row).apply {   limit(recordLen)} )

}