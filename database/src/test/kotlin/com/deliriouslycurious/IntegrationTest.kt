package com.deliriouslycurious

import com.natpryce.hamkrest.absent
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.present
import org.junit.BeforeClass
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.File
import java.util.concurrent.Callable
import java.util.concurrent.Executors

abstract class IntegrationTest {

    companion object {
        private val storageDirectory = File("src/test/resources/ephemeral-directory")

        @BeforeClass
        @JvmStatic
        fun removeTempDirectories() {
            storageDirectory.deleteRecursively()
            storageDirectory.mkdir()
        }

    }

    private val tempStorageDirectory = createTempDir(directory = storageDirectory)
    private val database = createDatabase(tempStorageDirectory)


    @Test
    fun `write 100 small records`() {

        IntRange(0, 100).forEach {
            val key = StringKey("small-" + it.toString())
            database.insert(Pair(key, TestDataFixture.KbData()))
        }

        IntRange(0, 100).forEach {
            val key = StringKey("small-" + it.toString())
            check(database.get(key), TestDataFixture.KbData())
        }
    }

    @Test
    fun `write 100 small records concurrently`() {

        val writes = IntRange(0, 100).map {
            val key = StringKey("small-concurrent-" + it.toString())
            Callable {
                database.insert(Pair(key, TestDataFixture.KbData()))
            }
        }

        Executors.newWorkStealingPool(4).invokeAll(writes).forEach { it.get() }

        IntRange(0, 100).forEach {
            val key = StringKey("small-concurrent-" + it.toString())
            check(database.get(key), TestDataFixture.KbData())
        }
    }

    @Test
    fun `write 100 big records`() {

        IntRange(0, 100).forEach {
            val key = StringKey("big-" + it.toString())
            database.insert(Pair(key, TestDataFixture.TenMbData()))
        }

        IntRange(0, 100).forEach {
            val key = StringKey("big-" + it.toString())
            check(database.get(key), TestDataFixture.TenMbData())
        }
    }

    @Test
    fun `write 100 big records concurrently`() {

        val writes = IntRange(0, 100).map {
            val key = StringKey("big-concurrent-" + it.toString())

            Callable {
                database.insert(Pair(key, TestDataFixture.TenMbData()))
            }
        }

        Executors.newWorkStealingPool(4).invokeAll(writes).forEach { it.get() }

        IntRange(0, 100).forEach {
            val key = StringKey("big-concurrent-" + it.toString())
            check(database.get(key), TestDataFixture.TenMbData())
        }
    }

    @Test
    fun `getting a non existant record returns null`() {
        val result = database.get(StringKey("doesnt-exist"))

        assertThat(result, absent())
    }

    @Test
    fun `two different records are both returned successfully`() {

        val record1Key = StringKey("key-1")
        val record1Data = "Some data that pertains to key-1 entry"
        val record2Key = StringKey("key-2")
        val record2Data = "Some data that pertains to key-2 entry"

        database.insert(Pair(record1Key, ByteData(record1Data.toByteArray())))
        database.insert(Pair(record2Key, ByteData(record2Data.toByteArray())))

        check(database.get(record1Key), ByteData(record1Data.toByteArray()))
        check(database.get(record2Key), ByteData(record2Data.toByteArray()))
    }

    @Test
    fun `correct record is deleted`() {

        val record1Key = StringKey("key-1")
        val record1Data = "Some data that pertains to key-1 entry"
        val record2Key = StringKey("key-2")
        val record2Data = "Some data that pertains to key-2 entry"

        database.insert(Pair(record1Key, ByteData(record1Data.toByteArray())))
        database.insert(Pair(record2Key, ByteData(record2Data.toByteArray())))

        check(database.get(record1Key), ByteData(record1Data.toByteArray()))
        check(database.get(record2Key), ByteData(record2Data.toByteArray()))

        database.delete(record1Key)

        assertThat(database.get(record1Key), absent())
        check(database.get(record2Key), ByteData(record2Data.toByteArray()))
    }

    abstract fun createDatabase(file: File): LittleDatabase
}

fun check(actual: Data?, expected: Data) {
    assertThat(actual, present())
    val actualData = ByteArrayInputStream(actual?.value()).bufferedReader().readText()
    val expectedData = ByteArrayInputStream(expected.value()).bufferedReader().readText()
    assertThat(actualData.length, equalTo(expectedData.length))
    assertThat(actualData, equalTo(expectedData))
}

fun check(message: String, actual: Data?, expected: Data) {
    assertThat(message, actual, present())
    val actualData = ByteArrayInputStream(actual?.value()).bufferedReader().readText()
    val expectedData = ByteArrayInputStream(expected.value()).bufferedReader().readText()
    assertThat(message, actualData.length, equalTo(expectedData.length))
    assertThat(message, actualData, equalTo(expectedData))
}

class TestDataFixture {

    companion object {

        fun KbData(): Data {
            return createDataWithSize(1024)
        }

        fun MbData(): Data {
            return createDataWithSize(1_048_576)
        }

        fun TenMbData(): Data {
            return createDataWithSize(10_485_760)
        }

        fun createDataWithSize(size: Int): Data = createData('A', size)

        fun createData(asciiCharacter: Char, size: Int): Data = ByteData(asciiCharacter.toString().repeat(size).toByteArray())
    }

}