package com.deliriouslycurious.records

interface RecordsSink: (Sequence<Record>) -> Unit

class FileRecordsSink: RecordsSink {
    override fun invoke(records: Sequence<Record>) {
        error("not implemented")
    }
}