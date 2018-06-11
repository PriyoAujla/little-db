package com.deliriouslycurious

import java.io.File

class InMemoryThenToFileDatabaseIntegrationTest: IntegrationTest() {
    override fun createDatabase(file: File): LittleDatabase =
            InMemoryThenToFileDatabase(databaseFilesPath = file)


}