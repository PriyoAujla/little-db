package com.deliriouslycurious

import java.io.File

class FileOnlyDatabaseIntegrationTest : IntegrationTest() {
    override fun createDatabase(file: File): LittleDatabase  = FileOnlyDatabase(file)
}