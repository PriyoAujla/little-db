package com.deliriouslycurious.records

import java.util.concurrent.ConcurrentLinkedDeque

interface RecordsSink: (Sequence<Record>) -> Unit

internal class FileRecordsSink(private val databaseFiles: DatabaseFiles,
                               private val fileRecords: ConcurrentLinkedDeque<FileRecords>
): RecordsSink {

    override fun invoke(records: Sequence<Record>) {
        val newDatabaseTableFile = databaseFiles.new()
        records.forEach {
            newDatabaseTableFile.appendBytes(it.asBytes())
        }
        fileRecords.addFirst(FileRecords(newDatabaseTableFile))
    }
}