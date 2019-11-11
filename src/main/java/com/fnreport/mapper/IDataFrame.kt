package com.fnreport.mapper

import org.intellij.lang.annotations.Language



/**
      - [x]   memory mapped
      - [x]   sparse
      - [x]   slices
      - [x]   pivot
      - [x]   group(by)
      - [ ]   time-resample
      - [ ]   one-hot columns
      - [ ]   coroutines/concurrent
      - [-]   lazy sequences (revisit with better rendering)
       */
interface IDataFrame : Iterable<Any?> {
    val size: Int
    val doc_string
        @Language("Markdown")
        get() = """
  - [x]   memory mapped
  - [x]   sparse
  - [x]   slices
  - [x]   pivot
  - [x]   group(by)
  - [ ]   time-resample
  - [ ]   one-hot columns 
  - [ ]   coroutines/concurrent
  - [-]   lazy sequences (revisit with better rendering)
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