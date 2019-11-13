package com.fnreport.mapper

import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

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
                    it.let { (column, codec, coord): Decoder ->
                        val input = coord.let { (begin, end): Pair<Int, Int> ->
                            ByteArray(end - begin).also {
                                mappedByteBuffer.apply { position(offset + begin) }[it]
                            }
                        }
                        val any = codec(*input)
                        column(any)
                    }
                }
            }

    override fun get(vararg c: Int): RowStore =let{origin ->

        object:IDataFrame {
            override fun invoke(row: Int) = origin(row).let { v -> (v as? List<*>)?.let { c.map(v::get) } ?: null }


            override fun get(vararg c: Int): RowStore {  origin.columns[c[]]  }

            override val columns: List<Column>
                get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
        }



