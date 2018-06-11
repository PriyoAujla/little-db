package com.deliriouslycurious

import java.util.concurrent.TimeUnit

fun <T> time(block: () -> T): T {
    val start = System.nanoTime()
    val result = block()
    val end = System.nanoTime()
    println("Time taken: ${TimeUnit.NANOSECONDS.toMillis(end-start)}")
    return result
}