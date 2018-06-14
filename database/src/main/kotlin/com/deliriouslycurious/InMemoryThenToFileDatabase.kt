package com.deliriouslycurious

import com.deliriouslycurious.Status.*
import com.deliriouslycurious.records.DatabaseFiles
import com.deliriouslycurious.records.FileRecords
import com.deliriouslycurious.records.FileRecordsSink
import com.deliriouslycurious.records.InMemoryRecords
import java.io.File
import java.util.*
import java.util.concurrent.atomic.AtomicInteger


private val defaultSizeOf128Mb = 134_217_728

class InMemoryThenToFileDatabase(sizeOfMemoryTable: Int = defaultSizeOf128Mb,
                                 databaseFilesPath: File
) : LittleDatabase {

    private val maxKeySize = 4098
    private val databaseFiles = DatabaseFiles(databaseFilesPath)
    private val inMemoryRecords = InMemoryRecords(sizeOfMemoryTable, databaseFilesPath)
    private val fileRecords = FileRecords(databaseFiles)

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
                    inMemoryRecords.migrateRecords(result.spaceRequired, FileRecordsSink(databaseFiles, fileRecords))
                }
                is RecordIsTooLarge -> throw Exception("Record is too large $result")
            }
        } while (result !is Successful)
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