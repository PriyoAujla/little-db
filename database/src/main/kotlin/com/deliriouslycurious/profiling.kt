package com.deliriouslycurious

import com.codahale.metrics.Histogram
import com.codahale.metrics.MetricRegistry
import java.util.concurrent.TimeUnit

fun <T> time(block: () -> T): T {
    val start = System.nanoTime()
    val result = block()
    val end = System.nanoTime()
    println("Time taken: ${TimeUnit.NANOSECONDS.toMillis(end-start)}")
    return result
}

object Profiling {
    val metrics = MetricRegistry()
    val fileKeySearchHistogram: Histogram = metrics.histogram("file-key-search-histogram")

    val getResponseTimes = metrics.timer("get-response-times")
}