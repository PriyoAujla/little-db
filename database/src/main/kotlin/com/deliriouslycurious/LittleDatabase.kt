package com.deliriouslycurious

import java.util.*

interface LittleDatabase {

    fun insert(record: Pair<Key, Data>)

    fun get(key: Key): Data?

    fun delete(key: Key)
}

interface Key {
    fun value(): String
    fun valueAsBytes(): ByteArray
}
interface Data {
    fun value(): ByteArray
}

data class StringKey(private val value: String): Key {
    companion object {
        fun from(byteArray: ByteArray): StringKey = StringKey(String(byteArray))
    }
    override fun valueAsBytes(): ByteArray = value().toByteArray()
    override fun value(): String = value
}

data class ByteData(private val value: ByteArray): Data {
    override fun value(): ByteArray = value

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ByteData

        if (!Arrays.equals(value, other.value)) return false

        return true
    }

    override fun hashCode(): Int = Arrays.hashCode(value)
}