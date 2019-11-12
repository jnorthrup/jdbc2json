package com.fnreport.mapper

import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel


interface RowStore {

    /**
     * seek to row
     */
    operator fun invoke(row: Int): Any?
}

interface FixedRowStore : RowStore {
    val recordLen: Int
    val size: Int
}


interface Column {
    val name: String
    operator fun invoke(any: Any?) = any
}

interface ColumnAccess {
    operator fun get(vararg c: Int): RowStore
    val columns: List<Column>
}


abstract class FileAccess(open val codex: List<Decoder>, open val filename: String) : ColumnAccess, Closeable {
    override val columns: List<Column> = codex.map { (c) -> c }
}

interface Codec {
    operator fun invoke(vararg input: Byte): Any?
}
typealias Coordinates = Pair<Int, Int>

interface IDataFrame : RowStore, ColumnAccess {}

data class Decoder(val col: Column, val codec: Codec, val coord: Coordinates)

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
) : FixedRowStore, FileAccess(codex, filename), IDataFrame, Closeable by randomAccessFile {

    override fun invoke(row: Int) =
            (  row * recordLen ).let { offset ->
                codex.map {
                    it.let { (col, cod, coord): Decoder ->
                        col(cod(*coord.let { (begin, end): Pair<Int, Int> ->
                            ByteArray(end - begin).also {
                                mappedByteBuffer.position(offset  + begin).get(it)
                            }
                        }))
                    }
                }
            }
    override fun get(vararg c: Int): RowStore {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}