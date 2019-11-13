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

