package com.deliriouslycurious

import com.deliriouslycurious.records.ByteOffset
import com.deliriouslycurious.records.Record
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue

class LogWriter(databaseFilesPath: File) {

    val logFile = databaseFilesPath.resolve("imtable.log")
    private val commands = LinkedBlockingQueue<LogCommand>(4098)
    private val commandExecutor = object : Thread() {
        override fun run() {
            while (true) {
                val command = commands.take()

                when (command) {
                    is LogCommand.WriteRecord -> {
                        writeRecordToLog(command)
                    }
                    is LogCommand.ExpungeData -> {
                        expungeFile()
                    }
                }
            }
        }
    }.start()

    fun addAction(command: LogCommand) {
        commands.put(command)
    }

    private fun writeRecordToLog(writeRecord: LogCommand.WriteRecord) {
        val channel = RandomAccessFile(logFile, "rw").channel
        channel.use {
            channel.write(ByteBuffer.wrap(writeRecord.record.asBytes()), writeRecord.offset.value)
        }
    }

    private fun expungeFile() {
        logFile.delete()
    }

    sealed class LogCommand {
        data class WriteRecord(val record: Record, val offset: ByteOffset) : LogCommand()
        class ExpungeData : LogCommand()
    }
}