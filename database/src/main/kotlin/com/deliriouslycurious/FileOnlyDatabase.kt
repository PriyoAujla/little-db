package com.deliriouslycurious

import java.io.File

/**
 * I wanted to use a basic version that simply leans on the File System
 * so I can validate concurrency tests written in the IntegrationTest class.
 */
class FileOnlyDatabase(private val storageDirectory: File): LittleDatabase {

    override fun insert(record: Pair<Key, Data>) {
        val fileToStoreData = storageDirectory.resolve(record.first.value())
        fileToStoreData.writeBytes(record.second.value())
    }

    override fun get(key: Key): Data? {
        val dataFile = storageDirectory.resolve(key.value())
        return if(dataFile.exists()) {
            ByteData(dataFile.readBytes())
        } else {
            null
        }
    }

    override fun delete(key: Key) {
        storageDirectory.resolve(key.value()).delete()
    }

}