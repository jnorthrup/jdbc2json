package com.fnreport.mapper

import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

typealias Column = Pair<String, (Any?) -> Any?>
typealias Coordinates = Pair<Int, Int>
typealias Decoder = Triple<Column, (ByteArray) -> Any?, Coordinates>

interface RowStore<T> {

    /**
     * seek to row
     */

    operator fun invoke(row: Int): T
}


interface FixedRowStore<T> : RowStore<T> {
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
//        codex: List<Decoder>,
        filename: String,
        randomAccessFile: RandomAccessFile = RandomAccessFile(filename, "r"),
        channel: FileChannel = randomAccessFile.channel,
        length: Long = randomAccessFile.length(),
        val mappedByteBuffer: MappedByteBuffer = channel.map(FileChannel.MapMode.PRIVATE, 0, length)
//        val recordLen: Int = mappedByteBuffer.run {
//            var c = 0.toByte();
//            do c = get() while (c != '\n'.toByte())
//            position()
//        },
//        val size: Int = (recordLen / length).toInt()//d functions use File | Settings | File Templates.
) : RowStore<ByteBuffer>, FileAccess(
        filename), Closeable by randomAccessFile {
    override fun invoke(row: Int): ByteBuffer = mappedByteBuffer.apply { position(row) }.slice()
}


//todo: map multiple segments for a very big file

class FixedLineStore(/*override val columns: List<Column> =listOf(  Column("line", { any: Any? -> (any as? Lazy<*>)?.value ?: any })),*/
                     val origin: MappedFile, override val recordLen: Int, override val size: Int) : FixedRowStore<Lazy<ByteBuffer>> {
    override fun invoke(row: Int): Lazy<ByteBuffer> = lazyOf(origin.invoke(row * recordLen).limit(recordLen) as ByteBuffer)
//    override fun get(vararg c: Int)  =this;
}


/*

    override fun invoke(row: Int) =
            (row * recordLen).let { offset ->
                codex.map { decoder: Decoder ->
                    decoder.let { (column: Column, codec, coord: Coordinates) ->
                        val (name, reifier) = column
                        val input = coord.let { (begin, end): Pair<Int, Int> ->
                        }
                        val any = codec(input)
                        reifier(any)
                    }
                }
            }

    override fun get(vararg c: Int): RowStore = let { origin: IDataFrame ->
        object : IDataFrame {
            override fun invoke(row: Int): List<Any?> {

            }

            override fun get(vararg c: Int): RowStore {
 c.map {origin.columns[c]}

            }

            override val columns: List<Column>
                get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
        }
    }
}

*/



