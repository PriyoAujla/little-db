package com.deliriouslycurious

import com.deliriouslycurious.Status.*
import com.deliriouslycurious.records.*
import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque


private val defaultSizeOf128Mb = 134_217_728

class InMemoryThenToFileDatabase(sizeOfMemoryTable: Int = defaultSizeOf128Mb,
                                 databaseFilesPath: File
) : LittleDatabase {

    private val maxKeySize = 4098
    private val databaseFiles = DatabaseFiles(databaseFilesPath)
    private val inMemoryRecords = InMemoryRecords(sizeOfMemoryTable, databaseFilesPath)
    private val fileRecords: ConcurrentLinkedDeque<FileRecords> = ConcurrentLinkedDeque(databaseFiles.files())


    override fun insert(record: Pair<Key, Data>) {
        record.first.validateSizeOrThrow()
        tryUpdatingDatabase {
            inMemoryRecords.add(record.first, record.second)
        }
    }

    override fun get(key: Key): Data? {
        val stopWatch = Profiling.getResponseTimes.time()
        val record = inMemoryRecords.get(key) ?: tryFileRecords(key)
        val result =  if (record == null || record.metaData.recordInfo.deleted()) {
            null
        } else {
            record.data
        }
        stopWatch.stop()
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
                    inMemoryRecords.migrateRecords(result.spaceRequired, FileRecordsSink(databaseFiles, fileRecords))
                }
                is RecordIsTooLarge -> throw Exception("Record is too large $result")
            }
        } while (result !is Successful)
    }

    private fun tryFileRecords(key: Key): Record? {
        val filesIterator = fileRecords.iterator()
        while (filesIterator.hasNext()) {
            val file = filesIterator.next()
            val record = file.get(key)

            if (record != null) {
                return record
            }
        }

        return null
    }
}

internal sealed class Status {
    class Successful : Status()
    data class InsufficientSpace(val spaceRequired: Int, val spaceLeft: Int) : Status()
    class MigratedRecords : Status()
    class TryReInserting : Status()
    data class RecordIsTooLarge(val maxSize: Int, val recordSize: Int) : Status()
    data class KeyIsTooLarge(val maxSize: Int, val keySize: Int) : Status()
}

internal interface KeySearchCriteria : (Key) -> Boolean
internal class SpecifiedKey(val subject: Key) : KeySearchCriteria {
    override fun invoke(key: Key): Boolean =
            Arrays.equals(key.valueAsBytes(), subject.valueAsBytes())
}

internal val everything = object : KeySearchCriteria {
    override fun invoke(key: Key): Boolean = true
}