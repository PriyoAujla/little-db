package com.deliriouslycurious.records

import com.deliriouslycurious.*
import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.experimental.buildSequence

internal class InMemoryRecords(private val size: Int, private val databaseFilesPath: File) {

    private val aSuccess = Status.Successful()
    private val readEndPosition = AtomicInteger(0)
    private val writeEndPosition = AtomicInteger(0)
    private val logWriter = LogWriter(databaseFilesPath)
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

    fun migrateToFilesRecords(spaceNeededToWriteRecord: Int, fileRecords: FileRecords, nextTableNumber: AtomicInteger): Status {
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
                return Status.MigratedRecordsToFile(newDatabaseTableFile)
            } else {
                return Status.TryReInserting()
            }
        }
    }

    private fun addRecord(key: Key, data: Data, recordInfo: RecordInfo): Status {

        val record = Record.from(recordInfo, key, data)
        val spaceNeeded = record.sizeOfRecordInBytes()

        if (size < spaceNeeded) {
            return Status.RecordIsTooLarge(size, spaceNeeded)
        }

        var beginWritingFrom = writeEndPosition.get()
        val currentFreeSpace = size - beginWritingFrom
        if (currentFreeSpace < spaceNeeded) {
            return Status.InsufficientSpace(spaceNeeded, currentFreeSpace)
        }

        while (!spaceReserved(beginWritingFrom, spaceNeeded)) {
            beginWritingFrom = writeEndPosition.get()
            val currentFreeSpace = size - beginWritingFrom
            if (currentFreeSpace < spaceNeeded) {
                return Status.InsufficientSpace(spaceNeeded, currentFreeSpace)
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

private val emptyRecordInfo = RecordInfo(0x00.toByte())
private val emptyData = ByteData(ByteArray(0))