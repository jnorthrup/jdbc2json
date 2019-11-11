package com.fnreport

import java.nio.ByteBuffer

abstract class ByteDataFrame(
    private val codex: Array<Decoder>,
    open val buffer: ByteBuffer,
    val recordLen: Int,
    override val size: Int,
    private var currentRow: Int = 0
    /*,  no apparent speedup at all over allocation.
    val trampoline: Array<ByteArray> = codexTrampoline(codex)*/
) : IDataFrame {

    override operator fun get(vararg select: Int) = select.map { codex[it] }.toTypedArray().let { newSelector: Codex ->
        SubFrame(newSelector, this)
    }

    override operator fun get(vararg select: String) =
        get(* codex.mapIndexed { index, (first) -> first.first() to index }.toMap().let { x ->
            select.map { x[it]!! }
        }.toIntArray()
        )

    override fun group(
        vararg gby: Int/*,todo
        vararg reducers: Array<out Pair<Int, (Any?) -> Any?>>*/
    ): IDataFrame = FrameGrouper(this, *gby )

    /** training wheels method leftover from iniitial experiments.
     */
    fun recordAsMapEntries(index: Int) = seekToRecord(index).let { (rowOffset, buf) ->
        codex.map { (field, mapper) ->
            field.second.let { (begin, end) ->
                try {
                    field.first() to reifyExtent(mapper, end - begin, buf.position(rowOffset + begin))
                } catch (e: Exception) {
                    System.err.println(
                        "" + mapOf(
                            "buf" to buf,
                            "currRow" to currentRow,
                            "index" to index,
                            "field" to field,
                            "mapper" to mapper
                        )
                    )
                    e.printStackTrace()
                    throw   Error("dead")
                }
            }
        }
    }

    override operator fun iterator() = (0 until size).map(this::invoke) .iterator()
    override operator fun invoke(row: Int) = codex .map { (field, mapper) ->
        val (rowOffset, buf) = seekToRecord(row)
        val (_, coords) = field
        val (begin, _) = coords
        reifyExtent(mapper, coords.size, buf.position(rowOffset + begin))
    }

    override fun invoke(vararg rows: Int) = rows.map { invoke(it) }


    fun recordAsList(index: Int) = seekToRecord(index).let { (rowOffset, buf) ->
        codex.map { (field, mapper) ->
            field.second.let { (begin, end) ->
                reifyExtent(mapper, end - begin, buf.position(rowOffset + begin))
            }
        }
    }


    override val columns
        get() = codex.map { (a, _) ->
            a.let { (col, _) -> col() }
        }

    /**lens syntax*/
    override operator fun get(select: Int, lens: (Any?) -> Any?) =

        SubFrame(codex.mapIndexed { index, decoder ->
            decoder.takeUnless { index == select } ?: decoder.let { (a, b) ->
                a to { ba: ByteArray -> lens(b(ba)) } as FieldParser<*>
            }
        }.toTypedArray(), this)

    fun seekToRecord(index: Int) = (recordLen * index).let { rowOffset ->
        currentRow = index
        rowOffset to buffer/*.position(rowOffset)*/
    }

    override fun pivot(untouched: Array<Int>, focalColumn: Int, vararg propogate: Int): IDataFrame {
        val subFrame: IDataFrame = get(focalColumn)
        val keyIndex = (0 until size).map { index ->
            val rows = index
            val firstOrNull = subFrame.invoke(rows)/*.firstOrNull()*/
            (firstOrNull as? String)?.intern() ?: firstOrNull
        }.toTypedArray()
        val decoders = keyIndex.toSet().let { keys ->
            codex[focalColumn].let { (metaDesc, _) ->
                val (keyprefixFunc) = metaDesc
                keyprefixFunc().let { _: String ->
                    propogate.map { codex[it] }.map { (propDesc, propParser) ->
                        val (propSuffixFunc: Function<String>, propCoordinates: Pair<Int, Int>) = propDesc
                        propSuffixFunc().let { _ ->
                            keys.map { keyInstance ->
                                Decoder({ "${keyprefixFunc()}:$keyInstance,${propSuffixFunc()}" } to propCoordinates) { bytes ->
                                    propParser.takeIf { keyIndex[currentRow] == keyInstance }?.invoke(bytes)
                                        ?: NullMapper(bytes)
                                }
                            }
                        }
                    }
                }
            }
        }.flatMap { it }.toTypedArray()
        return SubFrame(
            (untouched.map { codex[it] } +
                    decoders).toTypedArray(), this
        )
    }


    companion object {
        /**
         * for extracting field bytes we reuse trampoline buffers.
         */
        fun reifyExtent(
            mapper: FieldParser<*>,
            size: Int,
            buf: ByteBuffer
        ): Any? = /*trampoline.first { it.size == size }*/ByteArray(size).also { buf.get(it) }.let(mapper)
    }
}