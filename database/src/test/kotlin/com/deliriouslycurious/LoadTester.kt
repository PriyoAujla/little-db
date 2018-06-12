package com.deliriouslycurious

import com.codahale.metrics.ConsoleReporter
import com.natpryce.hamkrest.absent
import com.natpryce.hamkrest.assertion.assertThat
import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import java.io.BufferedInputStream
import java.io.File
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream


@Ignore("For help in profiling")
class LoadTester {

    private val storageDirectory = File("src/test/resources/ephemeral-directory")

    val reporter = ConsoleReporter.forRegistry(Profiling.metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build()

    @Before
    fun setup() {
        storageDirectory
                .listFiles()
                .filter { !it.isHidden }
                .forEach { it.deleteRecursively() }

        unzipAndCopyAllTo(File("src/test/resources/stress-test-file-only"), storageDirectory)
        unzipAndCopyAllTo(File("src/test/resources/stress-test-inmemory-only"), storageDirectory)
    }

    private fun unzipAndCopyAllTo(from: File, to: File) {
        val newDir = to.resolve(File(from.name))
        newDir.mkdir()
        from
                .listFiles()
                .forEach { file ->
                    val newFile = newDir.resolve(File(file.name.removeSuffix(".gz")))
                    newFile.outputStream()
                            .use { dataSink ->
                                BufferedInputStream(GZIPInputStream(file.inputStream())).use { dataSource ->
                                    dataSource.copyTo(dataSink)
                                }
                            }
                }
    }

    @After
    fun metrics() = reporter.report()

    @Test
    fun `stress test`() {
        val database = InMemoryThenToFileDatabase(databaseFilesPath = storageDirectory, sizeOfMemoryTable = 1_048_576)

        val agents = IntRange(0, 15).map {
            StressAgent("agent-$it", 5_000, database)
        }

        Executors.newWorkStealingPool(8).invokeAll(agents).forEach { it.get() }
    }


    @Test
    fun `calculate sequential writes per second to in memory table with 1mb inserts`() {
        val database = InMemoryThenToFileDatabase(databaseFilesPath = storageDirectory)

        val records = 100
        val testData = TestDataFixture.MbData()

        val start = System.nanoTime()
        IntRange(0, records).map {
            val key = StringKey("medium-" + it.toString())
            database.insert(Pair(key, testData))
        }
        val end = System.nanoTime()

        val recordsPerSecond = (records.toDouble() / TimeUnit.NANOSECONDS.toMillis(end - start)) * 1000
        println("Inserted $recordsPerSecond/s")
    }


    @Test
    fun `calculate concurrent writes per second to in memory table with 1mb inserts`() {
        val database = InMemoryThenToFileDatabase(databaseFilesPath = storageDirectory)

        val records = 100
        val testData = TestDataFixture.MbData()

        val writes = IntRange(0, records).map {
            val key = StringKey("medium-concurrent-" + it.toString())

            Callable {
                database.insert(Pair(key, testData))
            }
        }

        val start = System.nanoTime()
        Executors.newWorkStealingPool(4).invokeAll(writes).forEach { it.get() }
        val end = System.nanoTime()

        val recordsPerSecond = (records.toDouble() / TimeUnit.NANOSECONDS.toMillis(end - start)) * 1000
        println("Inserted $recordsPerSecond/s")
    }


    @Test
    fun `calculate sequential writes per second with 1mb inserts`() {
        val database = InMemoryThenToFileDatabase(databaseFilesPath = storageDirectory)

        val records = 1000
        val testData = TestDataFixture.MbData()

        val start = System.nanoTime()
        IntRange(0, records).map {
            val key = StringKey("medium-" + it.toString())
            database.insert(Pair(key, testData))
        }
        val end = System.nanoTime()

        val recordsPerSecond = (records.toDouble() / TimeUnit.NANOSECONDS.toMillis(end - start)) * 1000
        println("Inserted $recordsPerSecond/s")
    }


    @Test
    fun `calculate concurrent writes per second with 1mb inserts`() {
        val database = InMemoryThenToFileDatabase(databaseFilesPath = storageDirectory)

        val records = 1000
        val testData = TestDataFixture.MbData()
        val writes = IntRange(0, records).map {
            val key = StringKey("medium-concurrent-" + it.toString())

            Callable {
                database.insert(Pair(key, testData))
            }
        }

        val start = System.nanoTime()
        Executors.newWorkStealingPool(4).invokeAll(writes).forEach { it.get() }
        val end = System.nanoTime()

        val recordsPerSecond = (records.toDouble() / TimeUnit.NANOSECONDS.toMillis(end - start)) * 1000
        println("Inserted $recordsPerSecond/s")
    }


    @Test
    fun `calculate sequential reads per second from in memory table with 1mb inserts`() {
        val database = InMemoryThenToFileDatabase(databaseFilesPath = storageDirectory.resolve(File("stress-test-inmemory-only")))

        val records = 80
        val testData = TestDataFixture.MbData()

        0.until(10).forEach {
            val start = System.nanoTime()
            IntRange(0, records).forEach {
                val key = StringKey("medium-" + it.toString())
                check(database.get(key), testData)
            }
            val end = System.nanoTime()

            val recordsPerSecond = (records.toDouble() / TimeUnit.NANOSECONDS.toMillis(end - start)) * 1000
            println("Read $recordsPerSecond/s")
        }

    }


    @Test
    fun `calculate concurrent reads per second from in memory table with 1mb inserts`() {
        val database = InMemoryThenToFileDatabase(databaseFilesPath = storageDirectory.resolve(File("stress-test-inmemory-only")))

        val records = 80
        val testData = TestDataFixture.MbData()

        0.until(10).forEach {
            val reads = IntRange(0, records).map {

                Callable {
                    val key = StringKey("medium-" + it.toString())
                    check(database.get(key), testData)
                }
            }

            val start = System.nanoTime()
            Executors.newWorkStealingPool(4).invokeAll(reads).forEach { it.get() }
            val end = System.nanoTime()

            val recordsPerSecond = (records.toDouble() / TimeUnit.NANOSECONDS.toMillis(end - start)) * 1000
            println("Read $recordsPerSecond/s")
        }
    }

    /*
     * Before SSTable
     * -- Histograms ------------------------------------------------------------------
        file-key-search-histogram
                     count = 889
                       min = 0
                       max = 126
                      mean = 63.00
                    stddev = 36.68
                    median = 63.00
                      75% <= 95.00
                      95% <= 120.00
                      98% <= 124.00
                      99% <= 125.00
                    99.9% <= 126.00


    * After SSTable and middle offset only
    * -- Histograms ------------------------------------------------------------------
        file-key-search-histogram
                     count = 889
                       min = 0
                       max = 63
                      mean = 31.25
                    stddev = 18.34
                    median = 31.00
                      75% <= 47.00
                      95% <= 60.00
                      98% <= 62.00
                      99% <= 62.00
                    99.9% <= 63.00

     * After SSTable and 4 offsets
     *-- Histograms ------------------------------------------------------------------
        file-key-search-histogram
                     count = 889
                       min = 0
                       max = 31
                      mean = 15.38
                    stddev = 9.17
                    median = 15.00
                      75% <= 23.00
                      95% <= 30.00
                      98% <= 31.00
                      99% <= 31.00
                    99.9% <= 31.00

      * After SSTable and 10 offsets
      *-- Histograms ------------------------------------------------------------------
        file-key-search-histogram
                     count = 889
                       min = 0
                       max = 12
                      mean = 5.86
                    stddev = 3.67
                    median = 6.00
                      75% <= 9.00
                      95% <= 12.00
                      98% <= 12.00
                      99% <= 12.00
                    99.9% <= 12.00

     */
    @Test
    fun `calculate sequential reads per second from file table with 1mb inserts`() {
        val database = InMemoryThenToFileDatabase(databaseFilesPath = storageDirectory.resolve(File("stress-test-file-only")))

        val records = 888
        val testData = TestDataFixture.MbData()

        0.until(1).forEach {
            IntRange(0, records).forEach {
                val key = StringKey("medium-" + it.toString())
                check(database.get(key), testData)
            }

        }
    }


    @Test
    fun `calculate concurrent reads per second from file table with 1mb inserts`() {
        val database = InMemoryThenToFileDatabase(databaseFilesPath = storageDirectory.resolve(File("stress-test-file-only")))

        val records = 888
        val testData = TestDataFixture.MbData()

        0.until(1).forEach {

            val reads = IntRange(0, records).map {

                Callable {
                    val key = StringKey("medium-" + it.toString())
                    check(key.value(), database.get(key), testData)
                }
            }

            Executors.newWorkStealingPool(4).invokeAll(reads).forEach { it.get() }
        }

    }
}

private class StressAgent(val id: String, val inserts: Int, val database: LittleDatabase) : Callable<Unit> {

    private val testData = TestDataFixture.KbData()
    private val reReadAfterInsertCount = 5
    private val deletePreviousAfterInsertCount = 10

    override fun call() {

        IntRange(0, inserts).forEach {
            if (it % reReadAfterInsertCount == 0 && it != 0) {
                val key = StringKey("$id-${it - reReadAfterInsertCount}")
                val expectedData = ByteData(createData(key).toByteArray())
                val actualData = database.get(key)
                if (actualData != null) {
                    check(actualData, expectedData)
                } else {
                    System.err.println("Opppss $key is missing")
                }
            }

            if (it % deletePreviousAfterInsertCount == 0 && it != 0) {
                val key = StringKey("$id-${it - 1}")
                database.delete(key)
                assertThat(database.get(key), absent())
            }

            val key = StringKey("$id-$it")
            val data = createData(key)
            database.insert(Pair(key, ByteData(data.toByteArray())))
        }
    }

    private fun createData(key: StringKey) = "Some data that pertains to ${key.value()} entry " + testData

}