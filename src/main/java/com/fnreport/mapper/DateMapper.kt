package com.fnreport.mapper

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*

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
val <A : Int> Pair<A, A>.size get() = second - first
val <A : Int> Pair<A, A>.max get() = Integer.max(first, second)
private val dateTimeFormatter = DateTimeFormatter.ISO_DATE
val DoubleMapper: FieldParser<Double?> = {
    String(it).takeUnless(String::isBlank)?.trimEnd()?.toDouble()
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