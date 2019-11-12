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
