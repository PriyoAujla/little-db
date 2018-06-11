package com.deliriouslycurious

import com.codahale.metrics.Histogram
import com.codahale.metrics.MetricRegistry
import com.deliriouslycurious.Status.*
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.experimental.buildSequence


private val defaultSizeOf128Mb = 134_217_728

class InMemoryThenToFileDatabase(sizeOfMemoryTable: Int = defaultSizeOf128Mb,
                                 databaseFilesPath: File
) : LittleDatabase {

    private val maxKeySize = 4098
    private val inMemoryRecords = InMemoryRecords(sizeOfMemoryTable, databaseFilesPath)
    private val fileRecords = FileRecords(databaseFilesPath)

    override fun insert(record: Pair<Key, Data>) {
        record.first.validateSizeOrThrow()
        tryUpdatingDatabase {
            inMemoryRecords.add(record.first, record.second)
        }
    }

    override fun get(key: Key): Data? {
        val context = Profiling.getResponseTimes.time()
        val record = inMemoryRecords.get(key) ?: fileRecords.get(key)
        val result =  if (record == null || record.metaData.recordInfo.deleted()) {
            null
        } else {
            record.data
        }
        context.stop()
        return result
    }

    override fun delete(key: Key) {
        key.validateSizeOrThrow()
        tryUpdatingDatabase {
            inMemoryRecords.delete(key)
        }
    }

    private fun Key.validateSizeOrThrow() {
        val keySizeInBytes = valueAsBytes().size
        if (keySizeInBytes > maxKeySize) {
            throw Exception("Key is too big " + KeyIsTooLarge(maxKeySize, keySizeInBytes))
        }
    }

    private fun tryUpdatingDatabase(insertRecord: () -> Status) {
        do {
            val result = insertRecord()
            when (result) {
                is InsufficientSpace -> {
                    inMemoryRecords.migrateToFilesRecords(result.spaceRequired, fileRecords)
                }
                is RecordIsTooLarge -> throw Exception("Record is too large $result")
            }
        } while (result !is Successful)
    }
}

private class InMemoryRecords(private val size: Int, private val databaseFilesPath: File) {

    private val aSuccess = Successful()
    private val readEndPosition = AtomicInteger(0)
    private val writeEndPosition = AtomicInteger(0)
    private val logWriter = LogWriter(databaseFilesPath)
    private val nextTableNumber = AtomicInteger(DatabaseFiles.files(databaseFilesPath).count())
    @Volatile private var records: ByteArray = ByteArray(size).apply {
        if (logWriter.logFile.exists()) {
            val readBytes = logWriter.logFile.readBytes()
            System.arraycopy(readBytes, 0, this, 0, readBytes.size)
            readEndPosition.getAndSet(readBytes.size)
            writeEndPosition.getAndSet(readBytes.size)
        }
    }

    fun add(key: Key, data: Data): Status = addRecord(key, data, emptyRecordInfo)

    fun get(key: Key): Record? = fetchRecordsWhere(SpecifiedKey(key)).toList().lastOrNull()

    private fun all(): Sequence<Record> = fetchRecordsWhere(everything)

    private fun fetchRecordsWhere(keySearchCriteria: KeySearchCriteria): Sequence<Record> {

        return buildSequence {
            val byteBuffer: ByteBuffer = ByteBuffer.wrap(records).asReadOnlyBuffer()

            while (byteBuffer.position() < readEndPosition.get()) {

                val metaDataByteArray = ByteArray(MetaData.sizeOfMetaDataInBytes)
                byteBuffer.get(metaDataByteArray)

                val metaData = MetaData.from(metaDataByteArray)

                val keyByteArray = ByteArray(metaData.keyLength.value)
                byteBuffer.get(keyByteArray)

                if (keySearchCriteria(StringKey.from(keyByteArray))) {
                    val dataAsBytes = ByteArray(metaData.dataLength.value)

                    byteBuffer.get(dataAsBytes)

                    yield(Record(metaData, StringKey(String(keyByteArray)), ByteData(dataAsBytes)))
                } else {
                    byteBuffer.position(byteBuffer.position() + metaData.dataLength.value)
                }

            }

        }
    }

    fun delete(key: Key): Status = addRecord(key, emptyData, emptyRecordInfo.withDeleteBitSet())

    fun migrateToFilesRecords(spaceNeededToWriteRecord: Int, fileRecords: FileRecords): Status {
        synchronized(this) {
            val currentFreeSpace = size - writeEndPosition.get()
            // check again if migration is necessary for the given record size
            if (currentFreeSpace < spaceNeededToWriteRecord) {
                var lastKnownWriteEndPosition = writeEndPosition.get()

                // stop all writers from being able to write data to the table
                val spaceLeftInTable = size - lastKnownWriteEndPosition
                while (!spaceReserved(lastKnownWriteEndPosition, spaceLeftInTable)) {
                    lastKnownWriteEndPosition = writeEndPosition.get()
                }
                // now all writes will enter this synchronized block and be forced to wait for migration to complete

                while (readEndPosition.get() != lastKnownWriteEndPosition) {
                    // wait for any thread that managed to reserve space for it's record to finish writing the record
                }

                val recordsToSaveToFile = all().toList()
                        .groupBy { it.key }
                        .map { it.value.last() }
                        .sortedBy { it.key.value() }

                val newDatabaseTableFile = databaseFilesPath.resolve(DatabaseFiles.fileName(nextTableNumber.get()))
                recordsToSaveToFile.forEach {
                    newDatabaseTableFile.appendBytes(it.asBytes())
                }
                fileRecords.add(DatabaseFile(newDatabaseTableFile))

                nextTableNumber.incrementAndGet()
                readEndPosition.getAndSet(0)
                writeEndPosition.getAndSet(0)

                logWriter.addAction(LogWriter.LogCommand.ExpungeData())
                return MigratedRecordsToFile(newDatabaseTableFile)
            } else {
                return TryReInserting()
            }
        }
    }

    private fun addRecord(key: Key, data: Data, recordInfo: RecordInfo): Status {

        val record = Record.from(recordInfo, key, data)
        val spaceNeeded = record.sizeOfRecordInBytes()

        if (size < spaceNeeded) {
            return RecordIsTooLarge(size, spaceNeeded)
        }

        var beginWritingFrom = writeEndPosition.get()
        val currentFreeSpace = size - beginWritingFrom
        if (currentFreeSpace < spaceNeeded) {
            return InsufficientSpace(spaceNeeded, currentFreeSpace)
        }

        while (!spaceReserved(beginWritingFrom, spaceNeeded)) {
            beginWritingFrom = writeEndPosition.get()
            val currentFreeSpace = size - beginWritingFrom
            if (currentFreeSpace < spaceNeeded) {
                return InsufficientSpace(spaceNeeded, currentFreeSpace)
            }
        }

        val byteBuffer = ByteBuffer.wrap(records)
        byteBuffer.position(beginWritingFrom)
        byteBuffer.put(record.asBytes())

        val nextReadPosition = beginWritingFrom + spaceNeeded
        while (!readEndPosition.compareAndSet(beginWritingFrom, nextReadPosition)) {
            // do until set
        }
        logWriter.addAction(LogWriter.LogCommand.WriteRecord(record, ByteOffset.from(beginWritingFrom)))

        return aSuccess
    }

    private fun spaceReserved(beginWritingFrom: Int, spaceNeeded: Int) =
            writeEndPosition.compareAndSet(beginWritingFrom, beginWritingFrom + spaceNeeded)

}

private class FileRecords(databaseFilesPath: File) {

    private val files: ConcurrentLinkedDeque<DatabaseFile> = ConcurrentLinkedDeque(DatabaseFiles.files(databaseFilesPath))

    fun add(file: DatabaseFile) {
        files.addFirst(file)
    }

    fun get(key: Key): Record? {

        val filesIterator = files.iterator()
        while (filesIterator.hasNext()) {
            val file = filesIterator.next()
            val sequence = file.fetchRecordsWhere(SpecifiedKey(key))
            val record = sequence.use {
                it.lastOrNull()
            }

            if (record != null) {
                return record
            }
        }

        return null
    }

}

data class ByteOffset(val value: Long) {
    companion object {
        fun from(size: Int): ByteOffset {
            return ByteOffset(size.toLong())
        }
    }
}

private val emptyRecordInfo = RecordInfo(0x00.toByte())
private val emptyData = ByteData(ByteArray(0))

private sealed class Status {
    class Successful : Status()
    data class InsufficientSpace(val spaceRequired: Int, val spaceLeft: Int) : Status()
    data class MigratedRecordsToFile(val file: File) : Status()
    class TryReInserting : Status()
    data class RecordIsTooLarge(val maxSize: Int, val recordSize: Int) : Status()
    data class KeyIsTooLarge(val maxSize: Int, val keySize: Int) : Status()
}

private interface KeySearchCriteria : (Key) -> Boolean
private class SpecifiedKey(val subject: Key) : KeySearchCriteria {
    override fun invoke(key: Key): Boolean =
            Arrays.equals(key.valueAsBytes(), subject.valueAsBytes())
}

private val everything = object : KeySearchCriteria {
    override fun invoke(key: Key): Boolean = true

}

private class DatabaseFiles {
    companion object {

        private val extractFileNumber = Regex("table-([0-9]+).db")

        fun fileName(number: Int): String {
            return "table-$number.db"
        }

        fun files(databaseFilesPath: File): List<DatabaseFile> = databaseFilesPath
                .listFiles()
                .filter { it.name.endsWith(".db") }
                .mapNotNull { file ->
                    val fileNumber = extractFileNumber.find(file.name)?.groupValues?.get(1)?.toInt()
                    fileNumber?.let { Pair(it, file) }
                }
                .sortedByDescending { it.first }
                .map { DatabaseFile(it.second) }
    }
}


private class DatabaseFile(private val file: File) {

    private val index: SkipIndex

    init {
        index = SkipIndex.create(file, fetchRecordAndOffsetsWhere(everything))
    }

    fun fetchRecordsWhere(keySearchCriteria: KeySearchCriteria): CloseableSequence<Record> {
        return fetchRecordAndOffsetsWhere(keySearchCriteria).map { it.record }
    }

    fun fetchRecordAndOffsetsWhere(keySearchCriteria: KeySearchCriteria): CloseableSequence<RecordAndOffset> {
        val randomAccessFile = RandomAccessFile(file, "r")
        if(keySearchCriteria is SpecifiedKey) {
            val offset = index.offsetFor(keySearchCriteria.subject)
            randomAccessFile.seek(offset)
        }
        val channel = randomAccessFile.channel
        val sequence = buildSequence {
            var iterations = 0L
            val metaDataByteBuffer = ByteBuffer.allocate(MetaData.sizeOfMetaDataInBytes)
            while (channel.position() < channel.size()) {
                val offset = channel.position()
                channel.read(metaDataByteBuffer)
                val metadata = MetaData.from(metaDataByteBuffer.array())
                val keyAsBytes = ByteBuffer.allocate(metadata.keyLength.value)
                channel.read(keyAsBytes)
                val currentKey = StringKey(String(keyAsBytes.array()))

                if (keySearchCriteria(currentKey)) {
                    val dataAsBytes = ByteBuffer.allocate(metadata.dataLength.value)
                    channel.read(dataAsBytes)
                    if(keySearchCriteria is SpecifiedKey) {
                        Profiling.fileKeySearchHistogram.update(iterations)
                    }
                    yield(RecordAndOffset(Record.from(metadata.recordInfo, currentKey, ByteData(dataAsBytes.array())), offset))
                } else {
                    channel.position(channel.position() + metadata.dataLength.value)
                }

                metaDataByteBuffer.clear()
                iterations = iterations.inc()
            }
        }

        return CloseableSequence(channel, sequence)
    }

    private data class RecordAndOffset(val record: Record, val offset:Long)
    private data class KeyOffset(val key: Key, val offset: Long)

    class SkipIndex(private val offsets: List<KeyOffset> = emptyList(), private val endOfFileOffset: Long = 0L) {


        companion object {

            private val maxOffsets = 100

            fun create(file: File, records: CloseableSequence<RecordAndOffset>): SkipIndex {
                val bytesInFile = file.length()

                val offsets = records.use { sequence ->

                    val start = listOf(sequence.first())
                    val offsets = IntRange(1, maxOffsets).map { percentile ->
                        sequence.firstOrNull { it.offset > percentile * bytesInFile / maxOffsets }
                    }

                    (start + offsets)
                            .filterNotNull()
                            .map { KeyOffset(it.record.key, it.offset) }
                }

                return SkipIndex(offsets, bytesInFile)
            }
        }

        fun offsetFor(key: Key): Long {
            return offsets
                    .filter {
                        key.value() >= it.key.value()
                    }
                    .lastOrNull()?.offset ?: endOfFileOffset
        }
    }
}

private class CloseableSequence<out T>(val toClose: AutoCloseable, val sequence: Sequence<T>) : Sequence<T>, AutoCloseable by toClose {
    override fun iterator(): Iterator<T> = sequence.iterator()
}

private fun <T, R> CloseableSequence<T>.map(transform: (T) -> R):  CloseableSequence<R> {
    return CloseableSequence(toClose, sequence.map(transform))
}

object Profiling {
    val metrics = MetricRegistry()
    val fileKeySearchHistogram: Histogram = metrics.histogram("file-key-search-histogram")

    val getResponseTimes = metrics.timer("get-response-times")
}