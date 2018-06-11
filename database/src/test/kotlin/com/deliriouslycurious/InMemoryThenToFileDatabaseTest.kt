package com.deliriouslycurious

import com.natpryce.hamkrest.absent
import com.natpryce.hamkrest.assertion.assertThat
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import java.io.File


class InMemoryThenToFileDatabaseTest {

    companion object {
        private val storageDirectory = File("src/test/resources/ephemeral-directory")

        @BeforeClass
        @JvmStatic
        fun removeTempDirectories() {
            storageDirectory
                    .deleteRecursively()
            storageDirectory.mkdir()
        }

    }

    private val tempStorageDirectory = createTempDir(directory = storageDirectory)
    private val database = InMemoryThenToFileDatabase(2048, tempStorageDirectory)

    @Rule
    @JvmField
    val thrown = ExpectedException.none()

    @Test
    fun `add a record and retrieve it`() {
        val key = StringKey("some-key")
        val data = ByteData("some data to go with".toByteArray())
        database.insert(Pair(key, data))

        check(database.get(key), data)
    }

    @Test
    fun `add two records and retrieve the last one that was added`() {
        val key1 = StringKey("key-1")
        val data1 = ByteData("some data to go with key-1".toByteArray())
        val key2 = StringKey("key-2")
        val data2 = ByteData("some data to go with key-2".toByteArray())
        database.insert(Pair(key1, data1))
        database.insert(Pair(key2, data2))

        check(database.get(key2), data2)
    }

    @Test
    fun `add three records with varying lengths and retrieve`() {
        val key1 = StringKey("key-1")
        val data1 = ByteData("some short data".toByteArray())
        val key2 = StringKey("key-2")
        val data2 = ByteData("some data that is not too short but is concise".toByteArray())
        val key3 = StringKey("key-3")
        val data3 = ByteData("some data that is much longer and is stressing the logic to see how it copes with variable length data".toByteArray())
        database.insert(Pair(key3, data3))
        database.insert(Pair(key2, data2))
        database.insert(Pair(key1, data1))

        check(database.get(key1), data1)
        check(database.get(key2), data2)
        check(database.get(key3), data3)
    }

    @Test
    fun `can delete record`() {
        val key = StringKey("some-key")
        val data = ByteData("some data to go with".toByteArray())
        database.insert(Pair(key, data))

        check(database.get(key), data)

        database.delete(key)

        assertThat(database.get(key), absent())
    }

    @Test
    fun `can delete correct record`() {
        val key1 = StringKey("key-1")
        val data1 = ByteData("some short data".toByteArray())
        val key2 = StringKey("key-2")
        val data2 = ByteData("some data that is not too short but is concise".toByteArray())
        val key3 = StringKey("key-3")
        val data3 = ByteData("some data that is much longer and is stressing the logic to see how it copes with variable length data".toByteArray())
        database.insert(Pair(key3, data3))
        database.insert(Pair(key2, data2))
        database.insert(Pair(key1, data1))

        check(database.get(key1), data1)
        check(database.get(key2), data2)
        check(database.get(key3), data3)

        database.delete(key2)

        check(database.get(key1), data1)
        assertThat(database.get(key2), absent())
        check(database.get(key3), data3)
    }

    @Test
    fun `A record too large to fit in memory table will trigger creation of file based table`() {
        val key1 = StringKey("key-1")
        val data1 = ByteData("some short data".toByteArray())
        val key2 = StringKey("key-2")
        val data2 = ByteData("some data that is not too short but is concise".toByteArray())
        val key3 = StringKey("key-3")
        val data3 = TestDataFixture.KbData()
        val key4 = StringKey("key-4")
        val data4 = TestDataFixture.KbData()
        database.insert(Pair(key1, data1))
        database.insert(Pair(key2, data2))
        database.insert(Pair(key3, data3))
        database.insert(Pair(key4, data4))

        check(database.get(key1), data1)
        check(database.get(key2), data2)
        check(database.get(key3), data3)
        check(database.get(key4), data4)
    }

    @Test
    fun `Multiple files are created for successive migrations of in memory table to file`() {
        val key1 = StringKey("key-1")
        val data1 = ByteData("some short data".toByteArray())
        val key2 = StringKey("key-2")
        val data2 = ByteData("some data that is not too short but is concise".toByteArray())
        val key3 = StringKey("key-3")
        val data3 = TestDataFixture.KbData()
        val key4 = StringKey("key-4")
        val data4 = ByteData("some short data".toByteArray())
        val key5 = StringKey("key-5")
        val data5 = ByteData("some data that is not too short but is concise".toByteArray())
        val key6 = StringKey("key-6")
        val data6 = TestDataFixture.KbData()
        val key7 = StringKey("key-7")
        val data7 = ByteData("some short data".toByteArray())
        val key8 = StringKey("key-8")
        val data8 = ByteData("some data that is not too short but is concise".toByteArray())
        val key9 = StringKey("key-9")
        val data9 = TestDataFixture.KbData()
        database.insert(Pair(key1, data1))
        database.insert(Pair(key2, data2))
        database.insert(Pair(key3, data3))
        database.insert(Pair(key4, data4))
        database.insert(Pair(key5, data5))
        database.insert(Pair(key6, data6))
        database.insert(Pair(key7, data7))
        database.insert(Pair(key8, data8))
        database.insert(Pair(key9, data9))

        check(database.get(key1), data1)
        check(database.get(key2), data2)
        check(database.get(key3), data3)
        check(database.get(key4), data4)
        check(database.get(key5), data5)
        check(database.get(key6), data6)
        check(database.get(key7), data7)
        check(database.get(key8), data8)
        check(database.get(key9), data9)
    }

    @Test
    fun `A record too large to fit in memory table at all will throw exception`() {
        val key = StringKey("big-data-key")
        val data = TestDataFixture.TenMbData()

        thrown.expect(Exception::class.java)
        thrown.expectMessage("Record is too large RecordIsTooLarge(maxSize=2048, recordSize=10485781)")
        database.insert(Pair(key, data))
    }

    @Test
    fun `A delete entry too large to fit in memory table will trigger creation of file based table`() {
        val key1 = StringKey("key-1")
        val data1 = ByteData("some short data".toByteArray())
        val key2 = StringKey("key-2")
        val data2 = ByteData("some data that is not too short but is concise".toByteArray())
        val key3 = StringKey("key-3")
        val data3 = ByteData("some lorem ipsum funky stuff".toByteArray())
        val key4 = StringKey("key-4")
        val data4 = ByteData("<htm><head></head><body></body></html>".toByteArray())
        val key5 = StringKey("key-5")
        val data5 = ByteData("some data that is not too short but is concise".toByteArray())
        val key6 = StringKey("key-6")
        val data6 = TestDataFixture.createDataWithSize(560)
        val key7 = StringKey("key-7")
        val data7 = ByteData("filling out these strings with something to write can really be tedious :((((((".toByteArray())
        val key8 = StringKey("key-8")
        val data8 = ByteData("trying to type something so I can reach the limit of the ByteBuffer".toByteArray())
        val key9 = StringKey("key-9")
        val data9 = TestDataFixture.KbData()
        database.insert(Pair(key1, data1))
        database.insert(Pair(key2, data2))
        database.insert(Pair(key3, data3))
        database.insert(Pair(key4, data4))
        database.insert(Pair(key5, data5))
        database.insert(Pair(key6, data6))
        database.insert(Pair(key7, data7))
        database.insert(Pair(key8, data8))
        database.insert(Pair(key9, data9))
        database.delete(key9)
        database.delete(key2)

        check(database.get(key1), data1)
        assertThat(database.get(key2), absent())
        check(database.get(key3), data3)
        check(database.get(key4), data4)
        check(database.get(key5), data5)
        check(database.get(key6), data6)
        check(database.get(key7), data7)
        check(database.get(key8), data8)
        assertThat(database.get(key9), absent())
    }

    @Test
    fun `Key larger than max size is rejected when inserting`() {
        val database = InMemoryThenToFileDatabase(8196, tempStorageDirectory)
        val key = StringKey(String(TestDataFixture.createDataWithSize(4099).value()))
        val data = TestDataFixture.KbData()

        thrown.expect(Exception::class.java)
        thrown.expectMessage("Key is too big KeyIsTooLarge(maxSize=4098, keySize=4099)")

        database.insert(Pair(key, data))
    }

    @Test
    fun `Key larger than max size is rejected when deleting`() {
        val database = InMemoryThenToFileDatabase(8196, tempStorageDirectory)
        val key = StringKey(String(TestDataFixture.createDataWithSize(4099).value()))

        thrown.expect(Exception::class.java)
        thrown.expectMessage("Key is too big KeyIsTooLarge(maxSize=4098, keySize=4099)")

        database.delete(key)
    }

    @Test
    fun `Data is still accessible even after a database 'restart'`() {
        val restartedDatabase = InMemoryThenToFileDatabase(2048, File("src/test/resources/static-data"))

        val key1 = StringKey("key-1")
        val data1 = ByteData("some short data".toByteArray())
        val key2 = StringKey("key-2")
        val key3 = StringKey("key-3")
        val data3 = TestDataFixture.KbData()
        val key4 = StringKey("key-4")
        val data4 = ByteData("some short data".toByteArray())
        val key5 = StringKey("key-5")
        val data5 = ByteData("some data that is not too short but is concise".toByteArray())
        val key6 = StringKey("key-6")
        val data6 = TestDataFixture.KbData()
        val key7 = StringKey("key-7")
        val data7 = ByteData("some short data".toByteArray())
        val key8 = StringKey("key-8")
        val data8 = ByteData("some data that is not too short but is concise".toByteArray())
        val key9 = StringKey("key-9")
        val data9 = TestDataFixture.KbData()


        check(restartedDatabase.get(key1), data1)
        check(restartedDatabase.get(key3), data3)
        check(restartedDatabase.get(key4), data4)
        check(restartedDatabase.get(key5), data5)
        check(restartedDatabase.get(key6), data6)
        check(restartedDatabase.get(key7), data7)
        check(restartedDatabase.get(key8), data8)
        check(restartedDatabase.get(key9), data9)
        assertThat(restartedDatabase.get(key2), absent())
    }

    @Test
    fun `Not writing data in sequential lexographic order does not break anything`() {
        val key9 = StringKey("key-9")
        val data9 = ByteData("some short data".toByteArray())
        val key5 = StringKey("key-5")
        val data5 = ByteData("some data that is not too short but is concise".toByteArray())
        val key2 = StringKey("key-2")
        val data2 = ByteData("some lorem ipsum funky stuff".toByteArray())
        val key7 = StringKey("key-7")
        val data7 = ByteData("<htm><head></head><body></body></html>".toByteArray())
        val key3 = StringKey("key-3")
        val data3 = ByteData("some data that is not too short but is concise".toByteArray())
        val key6 = StringKey("key-6")
        val data6 = TestDataFixture.createDataWithSize(560)
        val key4 = StringKey("key-4")
        val data4 = ByteData("filling out these strings with something to write can really be tedious :((((((".toByteArray())
        val key8 = StringKey("key-8")
        val data8 = ByteData("trying to type something so I can reach the limit of the ByteBuffer".toByteArray())
        val key1 = StringKey("key-1")
        val data1 = TestDataFixture.KbData()
        database.insert(Pair(key9, data9))
        database.insert(Pair(key5, data5))
        database.insert(Pair(key2, data2))
        database.insert(Pair(key7, data7))
        database.insert(Pair(key3, data3))
        database.insert(Pair(key6, data6))
        database.insert(Pair(key4, data4))
        database.insert(Pair(key8, data8))
        database.insert(Pair(key1, data1))
        val expectedKey5Data = ByteData("updated key 5 data".toByteArray())
        database.insert(Pair(key5, expectedKey5Data))
        database.delete(key9)
        database.delete(key2)

        check(database.get(key1), data1)
        assertThat(database.get(key2), absent())
        check(database.get(key3), data3)
        check(database.get(key4), data4)
        check(database.get(key5), expectedKey5Data)
        check(database.get(key6), data6)
        check(database.get(key7), data7)
        check(database.get(key8), data8)
        assertThat(database.get(key9), absent())
    }

    @Test
    fun `A non existent record returns null`() {
        assertThat(database.get(StringKey("non-existing-key")), absent())
    }

    @Test
    fun `A key with records across multiple files returns the most recent record`() {
        val database = InMemoryThenToFileDatabase(1040, tempStorageDirectory)

        val key = StringKey("key-1")
        val expectedRecord = TestDataFixture.createData('B', 1024)
        database.insert(Pair(key, TestDataFixture.createData('C', 1024)))
        database.insert(Pair(key, TestDataFixture.createData('D', 1024)))
        database.insert(Pair(key, expectedRecord))
        database.insert(Pair(StringKey("key-2"), TestDataFixture.createData('E', 1024)))

        check(database.get(key), expectedRecord)

        val restartedDatabase = InMemoryThenToFileDatabase(1040, tempStorageDirectory)
        check(restartedDatabase.get(key), expectedRecord)
    }
}