package com.deliriouslycurious

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import org.junit.Test

class RecordInfoTest {

    @Test
    fun `record is not deleted when byte has no bits set`() {
        val recordInfo = RecordInfo(0x00.toByte())

        assertThat(recordInfo.deleted(), equalTo(false))
    }

    @Test
    fun `record is deleted only when delete bit is set (000000001)`() {
        assertThat(RecordInfo(0x01.toByte()).deleted(), equalTo(true))
        assertThat(RecordInfo(0x02.toByte()).deleted(), equalTo(false))
        assertThat(RecordInfo(0x03.toByte()).deleted(), equalTo(true))
    }

    @Test
    fun `set delete bit`() {

        assertThat(RecordInfo(0x00.toByte()).withDeleteBitSet(), equalTo(RecordInfo(0x01.toByte())))
        assertThat(RecordInfo(0x02.toByte()).withDeleteBitSet(), equalTo(RecordInfo(0x03.toByte())))
        assertThat(RecordInfo(0x04.toByte()).withDeleteBitSet(), equalTo(RecordInfo(0x05.toByte())))
    }
}