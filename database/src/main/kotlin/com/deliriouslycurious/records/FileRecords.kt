package com.deliriouslycurious.records

import com.deliriouslycurious.*
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.coroutines.experimental.buildSequence

internal class FileRecords(databaseFiles: DatabaseFiles) {

    private val files: ConcurrentLinkedDeque<DatabaseFile> = ConcurrentLinkedDeque(databaseFiles.files())

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

internal class DatabaseFiles(private val databaseFilesPath: File) {

    companion object {
        fun fileName(number: Int): String {
            return "table-$number.db"
        }
    }

    private val extractFileNumber = Regex("table-([0-9]+).db")

    fun new(): File = databaseFilesPath.resolve(fileName(files().count()))

    fun files(): List<DatabaseFile> = databaseFilesPath
            .listFiles()
            .filter { it.name.endsWith(".db") }
            .mapNotNull { file ->
                val fileNumber = extractFileNumber.find(file.name)?.groupValues?.get(1)?.toInt()
                fileNumber?.let { Pair(it, file) }
            }
            .sortedByDescending { it.first }
            .map { DatabaseFile(it.second) }
}

internal class DatabaseFile(private val file: File) {

    private val index: SkipIndex

    init {
        index = SkipIndex.create(file, fetchRecordAndOffsetsWhere(everything))
    }

    fun fetchRecordsWhere(keySearchCriteria: KeySearchCriteria): CloseableSequence<Record> {
        return fetchRecordAndOffsetsWhere(keySearchCriteria).map { it.record }
    }

    private fun fetchRecordAndOffsetsWhere(keySearchCriteria: KeySearchCriteria): CloseableSequence<RecordAndOffset> {
        val randomAccessFile = RandomAccessFile(file, "r")
        if (keySearchCriteria is SpecifiedKey) {
            val offset = index.offsetFor(keySearchCriteria.subject)
            randomAccessFile.seek(offset.value)
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
                    if (keySearchCriteria is SpecifiedKey) {
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

    private data class RecordAndOffset(val record: Record, val offset: Long)
    private data class KeyOffset(val key: Key, val offset: ByteOffset)

    private class SkipIndex(private val offsets: List<KeyOffset> = emptyList(), private val endOfFileOffset: ByteOffset = ByteOffset(0L)) {


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
                            .map { KeyOffset(it.record.key, ByteOffset(it.offset)) }
                }

                return SkipIndex(offsets, ByteOffset(bytesInFile))
            }
        }

        fun offsetFor(key: Key): ByteOffset {
            return offsets
                    .filter {
                        key.value() >= it.key.value()
                    }
                    .lastOrNull()?.offset ?: endOfFileOffset
        }
    }
}

internal class CloseableSequence<out T>(val toClose: AutoCloseable, val sequence: Sequence<T>) : Sequence<T>, AutoCloseable by toClose {
    override fun iterator(): Iterator<T> = sequence.iterator()
}

internal fun <T, R> CloseableSequence<T>.map(transform: (T) -> R): CloseableSequence<R> {
    return CloseableSequence(toClose, sequence.map(transform))
}