# My project's README

To use the database:

```
val database = InMemoryThenToFileDatabase(databaseFilesPath = File("/where-you-want-database-files-to-go/"))
database.insert(Pair(StringKey("some-key-value"), ByteData(someData.toBytes())))
database.get(StringKey("some-key-value"))

```