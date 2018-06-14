package com.deliriouslycurious.records

data class ByteOffset(val value: Long) {
    companion object {
        fun from(size: Int): ByteOffset {
            return ByteOffset(size.toLong())
        }
    }
}