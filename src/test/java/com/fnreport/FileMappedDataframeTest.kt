import com.fnreport.mapper.*
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class FileMappedDataframeTest {
    lateinit var mappedDataFrame: MappedFwf
    @Before
    fun setUp() {
val d1names = listOf("date", "channel", "deliver_qty", "return_qty")
        val x = arrayOf((0 to 10), (10 to 84), (84 to 124), (124 to 164))
        dlnames.zip(x.toList())
        val filemappers = mapOf(0 to DateMapper, 2 to DoubleMapper, 3 to DoubleMapper)
        this.mappedDataFrame = MappedFwf(

        )
    }

    @After
    fun tearDown() {
        mappedDataFrame.close()
    }

    @Test
    fun group() {
    }

    @Test
    operator fun iterator() {
    }

    @Test
    fun getColumns() {
        val columns = mappedDataFrame.columns
        Assert.assertEquals(4, columns.size)

    }

    @Test
    fun pivot() {

        val pivot = mappedDataFrame.pivot(arrayOf(0), 1, 2, 3)
        var p = pivot(0)as List<*>
        val columns = pivot.columns
         System.err.println(columns.zip(p))
    }

    @Test
    fun lens() {
        var l = mappedDataFrame[1, { (it as? String)?.substring(0 until 7) ?: it }]
        val l1 = l(1) as List<*>
        System.err.println(l1)
        System.err.println(l1[1])
        assertEquals("0102211", l1[1])
        var any = mappedDataFrame[1][0, { (it as? String)?.substring(0 until 7) ?: it }](1)
        System.err.println(any)
        assertEquals("0102211", (any as? List<*>)?.first() ?: any)
        any = (mappedDataFrame[1, { (it as? String)?.substring(0 until 7) ?: it }](1) as List<*>)[1]
        System.err.println(any)
        assertEquals("0102211", (any as? List<*>)?.first() ?: any)
        val pivot = mappedDataFrame[2,{it?:0.0}][3,{it?:0.0}].pivot(arrayOf(0), 1, 2, 3)(0)

        System.err.println(pivot)
    }
}