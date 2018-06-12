package com.deliriouslycurious.records

import com.deliriouslycurious.Data
import com.deliriouslycurious.Key
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.experimental.and
import kotlin.experimental.or

/*
    What is a record?

    +--------------------+
    |MetaData |Key | Data|
    +--------------------+

    MetaData:

    +---------------------------------------------------------------------------+
    |4-bytes for key length | 4-bytes for data length | 1 byte for deletion bit |
    +---------------------------------------------------------------------------+

    MetaData (Binary: 20 Bytes) = Length of key, Length of data and/or is it a delete entry
    Key (Binary: X bytes) = the key value
    Data (Binary: Y bytes) = the data value
 */

fun main(args: Array<String>) {

    val original = ByteBuffer.allocate(4).putInt(1024)

    val sliceArray = original.array().sliceArray(IntRange(2, 3))
    val afterBytes = ByteBuffer.allocate(4)
    afterBytes.order(ByteOrder.BIG_ENDIAN)
    afterBytes.put(0x00.toByte())
    afterBytes.put(0x00.toByte())
    afterBytes.put(sliceArray)

    println(original.getInt(0))
    println(afterBytes.getInt(0))

    println(listOf(
            "C", "B", "A", "CC", "BB", "AA", "CCC", "BBB", "AAA", "A1", "B1", "C1",
            "C10", "B10", "A10", "C100", "B100", "A100"
            ).sorted())

}

data class Record(val metaData: MetaData, val key: Key, val data: Data) {
    companion object {
        fun from(recordInfo: RecordInfo, key: Key, data: Data): Record {
            val keyAsBytes = key.valueAsBytes()
            val dataAsBytes = data.value()

            return Record(MetaData(KeyLength(keyAsBytes.size), DataLength(dataAsBytes.size), recordInfo), key, data)
        }
    }

    fun asBytes(): ByteArray = metaData.toByteArray() + key.valueAsBytes() + data.value()

    fun sizeOfRecordInBytes() = MetaData.sizeOfMetaDataInBytes + metaData.keyLength.value + metaData.dataLength.value
}

data class MetaData(val keyLength: KeyLength, val dataLength: DataLength, val recordInfo: RecordInfo) {

    companion object {
        val sizeOfMetaDataInBytes = 4 + 4 + 1

        fun from(metaDataAsBytes: ByteArray): MetaData {
            val keyLengthAsBytes = ByteBuffer.allocate(4)
            keyLengthAsBytes.order(ByteOrder.BIG_ENDIAN)
            keyLengthAsBytes.put(metaDataAsBytes.sliceArray(IntRange(0, 3)))
            val keyLength = KeyLength(keyLengthAsBytes.getInt(0))

            val dataLengthAsBytes = ByteBuffer.allocate(4)
            dataLengthAsBytes.order(ByteOrder.BIG_ENDIAN)
            dataLengthAsBytes.put(metaDataAsBytes.sliceArray(IntRange(4, 7)))
            val dataLength = DataLength(dataLengthAsBytes.getInt(0))

            val recordInfo = RecordInfo(metaDataAsBytes[8])

            return MetaData(keyLength, dataLength, recordInfo)
        }
    }

    fun toByteArray(): ByteArray {
        val keyLengthInBytes = ByteBuffer.allocate(4)
        keyLengthInBytes.putInt(keyLength.value)
        keyLengthInBytes.position(0)

        val dataLengthInBytes = ByteBuffer.allocate(4)
        dataLengthInBytes.putInt(dataLength.value)
        dataLengthInBytes.position(0)

        val result = ByteBuffer.allocate(sizeOfMetaDataInBytes)
        result.put(keyLengthInBytes)
        result.put(dataLengthInBytes)
        result.put(recordInfo.byte)
        return result.array()
    }
}

data class KeyLength(val value: Int)
data class DataLength(val value: Int)
data class RecordInfo(val byte: Byte) {

    private val deleteBitMask = 0x01.toByte()

    fun deleted(): Boolean = byte.and(deleteBitMask) == deleteBitMask

    fun withDeleteBitSet(): RecordInfo = RecordInfo(byte.or(deleteBitMask))
}