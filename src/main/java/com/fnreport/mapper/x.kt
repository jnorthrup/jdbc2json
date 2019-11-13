
package com.fnreport.mapper

import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

typealias Column=Pair<String,(Any?)->Any?>
typealias Coordinates=Pair<Int,Int>
typealias Decoder=Triple<Column,( ByteArray)->Any?,Coordinates>
interface RowStore {

/**
     * seek to row
     */

    operator fun invoke(row: Int): List< Any?>
}



interface FixedRowStore : RowStore {
    val recordLen: Int
    val size: Int
}
interface ColumnAccess {
    operator fun get(vararg c: Int): RowStore
    val columns: List<Column>
}

abstract class FileAccess(open val codex: List<Decoder>, open val filename: String) : Closeable {

}



interface IDataFrame : RowStore, ColumnAccess{
}



//todo: map multiple segments for a very big file
class MappedFwf(
        codex: List<Decoder>,
        filename: String,
        randomAccessFile: RandomAccessFile = RandomAccessFile(filename, "r"),
        channel: FileChannel = randomAccessFile.channel,
        length: Long = randomAccessFile.length(),
        private val mappedByteBuffer: MappedByteBuffer = channel.map(FileChannel.MapMode.PRIVATE, 0, length),
        override val recordLen: Int = mappedByteBuffer.run {
            var c = 0.toByte();
            do c = get() while (c != '\n'.toByte())
            position()
        },
        override val size: Int = (recordLen / length).toInt()
) : FixedRowStore, FileAccess(codex, filename),Closeable by randomAccessFile {
    override fun invoke(row: Int): List<Any?> { (row * recordLen).let { offset ->                            ByteArray(end - begin).also {
        mappedByteBuffer.apply { position(offset + begin) }[it]
    }



};


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



