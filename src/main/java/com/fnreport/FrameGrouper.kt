package com.fnreport

class FrameGrouper(
    private val origin: IDataFrame,
    vararg val gby: Int
) : IDataFrame {
    /**
     * when group frame renders a row it opens this array
     *
     */
    private val deflect by lazy {
        //collect
        val keys = origin.get(*gby).mapIndexed { i, x -> i to x  }.toMap()

        val clusters = linkedMapOf<Int, List<Int>>()
        //reduce
        keys.entries.forEach { (k, v) ->
            val hashCode = v.hashCode()

            clusters[hashCode] = clusters[hashCode]?.let { ints -> ints + (k) } ?: listOf(k)
        }
        clusters.entries.map { (_, v) ->
            v.toIntArray()
        }.toTypedArray()
    }

    override val size by lazy { deflect.size }
    override val columns: List<String>
        get() = origin.columns

    override fun group(gby: IntArray): IDataFrame {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun pivot(untouched: Array<Int>, focalColumn: Int, vararg propogate: Int): IDataFrame {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun get(select: Int, lens: (Any?) -> Any?): IDataFrame {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun get(vararg select: String): IDataFrame {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    val frames = columns.indices.map { i -> origin[i] }


    override fun invoke(row: Int): List<Any?> =
        run {
            deflect[row].let { vr ->
                (columns.indices).map { i ->
                    if (i in gby)
                        frames[i](vr.first())
                    else
                        frames[i](vr)
                }
            }
        }

    override fun iterator() =
        invoke(0 until size).toList().iterator()

}