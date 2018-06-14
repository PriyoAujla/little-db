package com.deliriouslycurious.records

interface RecordsSink: (Sequence<Record>) -> Unit

internal class FileRecordsSink(private val databaseFiles: DatabaseFiles,
                               private val filesRecords: FileRecords
): RecordsSink {

    override fun invoke(records: Sequence<Record>) {
        val newDatabaseTableFile = databaseFiles.new()
        records.forEach {
            newDatabaseTableFile.appendBytes(it.asBytes())
        }
        filesRecords.add(DatabaseFile(newDatabaseTableFile))
    }
}