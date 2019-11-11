package com.fnreport

import org.intellij.lang.annotations.Language
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*

typealias codec = (ByteArray) -> Any
typealias IndirectName = () -> String
typealias ParseCoordinates = Pair<Int, Int>
typealias FieldParser<T> = (ByteArray) -> T?
typealias ParseDescriptor = Pair<IndirectName, ParseCoordinates>
typealias Decoder = Pair<ParseDescriptor, FieldParser<*>>
typealias Codex = Array<Decoder>

val <A : Int> Pair<A, A>.size get() = second - first
val <A : Int> Pair<A, A>.max get() = Integer.max(first, second)
val StringMapper: FieldParser<String?> = {
    String(it).takeUnless(String::isBlank)?.trimEnd()
}
val IntMapper: FieldParser<Int?> = {
    String(it).takeUnless(String::isBlank)?.trimEnd()?.toInt()
}

private val dateTimeFormatter = DateTimeFormatter.ISO_DATE

val DateMapper: FieldParser<LocalDate?> = {
    var res: LocalDate? = null

    String(it).takeUnless(String::isBlank)?.trimEnd()?.let { s ->
        try {
            res = LocalDate.parse(s, dateTimeFormatter)
        } catch (e: Exception) {
            val parseBest = dateTimeFormatter.parseBest(s)
            res = LocalDate.from(
                parseBest
            )
        }
    }
    res

}

val LongMapper: FieldParser<Long?> = {
    String(it).takeUnless(String::isBlank)?.trimEnd()?.toLong()
}
val DoubleMapper: FieldParser<Double?> = {
    String(it).takeUnless(String::isBlank)?.trimEnd()?.toDouble()
}
val NullMapper: FieldParser<Any?> = {
    null
}


/**
 * return in order of frequency
 */
private fun codexTrampoline(codex: Codex): Array<ByteArray> {
    val sizes = TreeMap<Int, Int>()

    codex.map { (meta) ->
        val (_, coord) = meta
        coord.size
    }.forEach { x ->
        sizes[x] = (sizes[x] ?: 0) + 1
    }
    return sizes.entries.sortedBy { (_, v) -> v }.map { (key) -> ByteArray(key) }.toTypedArray()
}



interface IDataFrame : Iterable<Any?>  {
    val size: Int
    val doc_string
        @Language("Markdown")
        get() = """
    [x]   memory mapped
    [x]   sparse
    [x]   slices
    [x]   pivot
    [x]   group(by)
    [ ]   time-resample
    [ ]   one-hot columns 
    [ ]   coroutines/concurrent
    [-]   lazy sequences (revisit with better rendering)
    """.trimIndent()


    val columns: List<String>
    fun group(gby: IntArray/*, vararg reducers: Array<out Pair<Int, (Any?) -> Any?>>*/): IDataFrame
    fun pivot(untouched: Array<Int>, focalColumn: Int, vararg propogate: Int): IDataFrame
    operator fun get(select: Array<Int>) = get(select.toList())
    operator fun get(select: Iterable<Int>) = get(*select.map { it }.toIntArray())
    operator fun get(select: Int, lens: (Any?) -> Any?): IDataFrame
    operator fun get(vararg select: Int): IDataFrame = get(select.toList())
    operator fun get(vararg select: String): IDataFrame
    operator fun invoke(row: Int): Any?
    operator fun invoke(rows: IntArray) = rows.map { i -> invoke(i) }
    operator fun invoke(rows: IntRange) = rows.map { i -> invoke(i) }
}